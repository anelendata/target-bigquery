#!/usr/bin/env python3

import argparse, datetime, io, logging, re, sys, time
import simplejson as json

import singer

from tempfile import TemporaryFile

from google.cloud import bigquery
from google.cloud.bigquery.job import SourceFormat
from google.cloud.bigquery import Dataset, LoadJobConfig
from google.cloud.exceptions import NotFound
from google.api_core import exceptions

from target_bigquery.schema import parse_schema, clean_and_validate

logger = singer.get_logger()

MAX_WARNING = 20


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
        except exceptions.Conflict:
            logger.info("Failed to create table %s" % table_id)
            raise
        except exceptions.BadRequest:
            logger.error("Error creating table with schema: %s" % schema)
            raise

    return table


def write_records(project_id, dataset_name, lines=None,
                  stream=False, on_invalid_record="abort", partition_by=None,
                  numeric_type="NUMERIC"):
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
            state = message.value
            # State may contain sensitive info. Not logging in production
            logger.debug("State: %s" % state)
            currently_syncing = state.get("currently_syncing")
            bookmarks = state.get("bookmarks")
            if currently_syncing and bookmarks:
                logger.info("State: currently_syncing %s - last_update: %s" %
                            (currently_syncing,
                             bookmarks.get(currently_syncing, dict()).get(
                                 "last_update")))

        elif isinstance(message, singer.SchemaMessage):
            table_name = message.stream

            if schemas.get(table_name):
                # Redundant schema rows
                continue

            schemas[table_name] = message.schema
            bq_schema = parse_schema(schemas[table_name], numeric_type)
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

        if row_count[table_name] == 0:
            logger.info(f"Zero records for {table}. Skip loading.")
            continue
        logger.info(f"Batch loading {table} to Bigquery")
        table_files[table_name].seek(0)
        table_id = f"{project_id}.{dataset_name}.{table_name}"
        try:
            load_job = client.load_table_from_file(
                table_files[table_name], table_id, job_config=load_config)
        except exceptions.BadRequest:
            logger.error("Error loading records for table " + table_name)
            logger.error(bq_schema)
            table_files[table_name].seek(0)
            logger.debug(table_files[table_name].read())
            raise
        logger.info("Batch loading job {}".format(load_job.job_id))
        try:
            logger.debug(load_job.result())
        except Exception as e:
            logger.critical(load_job.errors)
            raise

    logger.info(f"{json.dumps(row_count)}")
    return state


def _emit_state(state):
    if state is None:
        return
    line = json.dumps(state)
    logger.debug("Emitting state {}".format(line))
    sys.stdout.write("{}\n".format(line))
    sys.stdout.flush()


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
                          partition_by=config.get("partition_by"),
                          numeric_type=config.get("numeric_type", "NUMERIC"))

    _emit_state(state)
    logger.debug("Exiting normally")

if __name__ == '__main__':
    main()
