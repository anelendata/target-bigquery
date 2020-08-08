#!/usr/bin/env python3

import argparse, datetime, io, json, logging, re, sys, time

from jsonschema import validate
from jsonschema.exceptions import ValidationError

import singer

from tempfile import TemporaryFile

from google.cloud import bigquery
from google.cloud.bigquery.job import SourceFormat
from google.cloud.bigquery import Dataset, LoadJobConfig
from google.cloud.exceptions import NotFound
from google.api_core import exceptions

from target_bigquery.schema import parse_schema

logger = singer.get_logger()

MAX_WARNING = 20

# StitchData compatible timestamp meta data
#  https://www.stitchdata.com/docs/data-structure/system-tables-and-columns
BATCH_TIMESTAMP = "_sdc_batched_at"


def clean_and_validate(message, schemas, invalids, on_invalid_record,
                       json_dumps=False):
    batch_tstamp = datetime.datetime.utcnow()
    batch_tstamp = batch_tstamp.replace(
        tzinfo=datetime.timezone.utc)

    if message.stream not in schemas:
        raise Exception(("A record for stream {} was encountered" +
                         "before a corresponding schema").format(
                             message.stream))

    schema = schemas[message.stream]

    try:
        validate(message.record, schema)
    except ValidationError as e:
        cur_validation = False
        error_message = str(e)

        # A hacky attempt to ignore number-convertible strings...
        instance = re.sub(r".*instance\[\'(.*)\'\].*", r"\1",
                          error_message.split("\n")[5])
        type_ = re.sub(r".*\{\'type\'\: \[\'.*\', \'(.*)\'\]\}.*",
                       r"\1", error_message.split("\n")[3])
        if type_ in ["integer", "number"]:
            n = None
            try:
                n = float(message.record[instance])
            except Exception:
                # In case we want to persist the rows with partially
                # invalid value
                message.record[instance] = None
                pass
            if n is not None:
                cur_validation = True
        if cur_validation is False:
            invalids = invalids + 1
            if invalids < MAX_WARNING:
                logger.warn(("Validation error in record %d [%s]" +
                             " :: %s :: %s :: %s") %
                            (count, instance, type_, str(message.record),
                             str(e)))
            elif invalids == MAX_WARNING:
                logger.warn("Max validation warning reached.")

            if on_invalid_record == "abort":
                raise ValidationError("Validation required and failed.")

    if BATCH_TIMESTAMP in schema["properties"].keys():
        message.record[BATCH_TIMESTAMP] = batch_tstamp.isoformat()

    record = message.record
    if json_dumps:
        record = bytes(json.dumps(record) + "\n", "UTF-8")

    return record, invalids



def get_or_create_dataset(client, project_id, dataset_name, location="US"):
    dataset_id = "%s.%s" % (project_id, dataset_name)
    try:
        dataset = client.get_dataset(dataset_id)
    except NotFound:
        try:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = location
            dataset = client.create_dataset(dataset)
            logger.info("Created a new dataset %s" % dataset_id)
        except exceptions.Conflict as e:
            logger.critical("Failed to both get and create dataset %s" %
                            dataset_id)
            raise e
    return dataset


def get_or_create_table(client, project_id, dataset_name, table_name, schema,
                        partition_by):
    table_id = "%s.%s.%s" % (project_id, dataset_name, table_name)
    try:
        table = client.get_table(table_id)
    except NotFound:
        logger.info("Creating a new table %s" % table_id)
        table = bigquery.Table(table_id, schema=schema)
        if partition_by:
            logger.info("Creating a partitioned table")
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_by,  # name of column to use for partitioning
                # expiration_ms=7776000000,  # 90 days
            )
        try:
            table = client.create_table(table)
        except exceptions.Conflict as e:
            logger.info("Failed to create table %s" % table_id)
            raise e

    return table


def write_records(project_id, dataset_name, lines=None,
                  stream=False, on_invalid_record="abort", partition_by=None):
    if on_invalid_record not in ("abort", "skip", "force"):
        raise ValueError("on_invalid_record must be one of" +
                         " (abort, skip, force)")

    state = None
    schemas = {}
    bq_schemas = {}
    tables = {}
    key_properties = {}
    table_files = {}
    row_count = {}
    errors = {}

    client = bigquery.Client(project=project_id)

    dataset = get_or_create_dataset(client, project_id, dataset_name)

    count = 0
    invalids = 0
    for line in lines:
        try:
            message = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(message, singer.RecordMessage):
            if stream:
                json_dumps = False
            else:
                json_dumps = True
            record, invalids = clean_and_validate(message, schemas, invalids,
                                                  on_invalid_record, json_dumps)

            if invalids == 0 or on_invalid_record == "force":
                # https://cloud.google.com/bigquery/streaming-data-into-bigquery
                if stream:
                    errors[message.stream] = client.insert_rows(
                        tables[message.stream], [record])
                else:
                    table_files[message.stream].write(record)

            row_count[message.stream] += 1
            state = None

        elif isinstance(message, singer.StateMessage):
            logger.info("State: %s" % message.value)
            state = message.value

        elif isinstance(message, singer.SchemaMessage):
            table_name = message.stream
            schemas[table_name] = message.schema
            bq_schema = parse_schema(schemas[table_name])
            bq_schemas[table_name] = bq_schema

            # My mom always said life was like a box of chocolates.
            # You never know what you're gonna get...or get streamed by a tap.
            # So be lazy in initialization
            tables[table_name] = get_or_create_table(client, project_id,
                                                     dataset_name,
                                                     table_name,
                                                     bq_schema,
                                                     partition_by)
            if stream:
                # Ensure the table is created before streaming...
                time.sleep(3)

            if not stream:
                table_files[table_name] = TemporaryFile(mode='w+b')

            key_properties[table_name] = message.key_properties
            row_count[table_name] = 0
            errors[table_name] = None

        elif isinstance(message, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception("Unrecognized message {}".format(message))

        count = count + 1

    # We already wrote the data in the streaming mode
    if stream:
        for table_name in errors.keys():
            if not errors[table_name]:
                logger.info("Streamed {} row(s) into {}.{}.{}".format(
                    row_count[table_name], project_id,
                    dataset_name, table_name))
            else:
                logger.warn("Errors:", errors[table], sep=" ")
        return state

    # For batch job mode only
    if invalids > 0:
        if on_invalid_record == "skip":
            logger.warn("Persisting data set by skipping the invalid records.")
        elif on_invalid_record == "force":
            logger.warn("Persisting data by replacing invalids with null.")

    for table_name in table_files.keys():
        bq_schema = bq_schemas[table_name]

        # We should already have get-or-created:
        table = tables[table_name]

        load_config = LoadJobConfig()
        load_config.schema = bq_schema
        load_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON

        logger.info("Batch loading {} to Bigquery.\n".format(table))
        table_files[table_name].seek(0)
        table_id = "%s.%s.%s" % (project_id, dataset_name, table_name)
        load_job = client.load_table_from_file(
            table_files[table_name], table_id, job_config=load_config)
        logger.info("Batch loading job {}".format(load_job.job_id))
        logger.info(load_job.result())

    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    on_invalid_record = config.get('on_invalid_record', "abort")

    input_ = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    state = write_records(config["project_id"],
                          config["dataset_id"],
                          input_,
                          stream=config.get("stream", False),
                          on_invalid_record=on_invalid_record,
                          partition_by=config.get("partition_by"))

    if state is not None:
        line = json.dumps(state)
        logger.debug("Emitting state {}".format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


if __name__ == '__main__':
    main()
