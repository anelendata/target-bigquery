import datetime
import simplejson as json
import re

from google.cloud.bigquery import Client, SchemaField
from jsonschema import validate, FormatChecker
from jsonschema.exceptions import ValidationError
import singer


# StitchData compatible timestamp meta data
#  https://www.stitchdata.com/docs/data-structure/system-tables-and-columns
BATCH_TIMESTAMP = "_sdc_batched_at"
JSONSCHEMA_TYPES = ["object", "array", "string", "integer", "number", "boolean"]

logger = singer.get_logger()


def _get_schema_type_mode(property_, numeric_type, integer_type):
    type_ = property_.get("type")

    jsonschema_type = "null"
    schema_type = "NULL"
    schema_mode = "NULLABLE"
    if isinstance(type_, list):
        if type_[0] != "null":
            schema_mode = "REQUIRED"

        if len(type_) < 2 or type_[1] not in JSONSCHEMA_TYPES:
            # Some major taps contain type first :(
            jsonschema_type = type_[0]
        else:
            jsonschema_type = type_[1]
    elif isinstance(type_, str):
        jsonschema_type = type_
    else:
        raise Exception(f"type must be given as string or list. Given {type(type_)}")

    jsonschema_type = jsonschema_type.lower()
    if jsonschema_type not in JSONSCHEMA_TYPES:
        raise Exception(f"{jsonschema_type} is not a valid jsonschema type. property: {property_}")

    # map jsonschema to BigQuery type
    if jsonschema_type == "object":
        schema_type = "RECORD"

    if jsonschema_type == "array":
        # Determined later by the item
        schema_type = None
        schema_mode = "REPEATED"

    if jsonschema_type == "string":
        schema_type = "STRING"
        if "format" in property_:
            if property_["format"] == "date-time":
                schema_type = "TIMESTAMP"
            elif property_["format"] == "json":
                schema_type = "JSON"

    if jsonschema_type == "integer":
        schema_type = integer_type

    if jsonschema_type == "number":
        schema_type = numeric_type

    if jsonschema_type == "boolean":
        schema_type = "BOOLEAN"

    return schema_type, schema_mode


def _parse_property(key, property_, numeric_type="NUMERIC", integer_type="INTEGER"):
    if numeric_type not in ["NUMERIC", "FLOAT64"]:
        raise ValueError("Unknown numeric type %s" % numeric_type)
    if integer_type not in ["INTEGER", "INT64"]:
        raise ValueError("Unknown integer type %s" % integer_type)

    schema_name = key
    schema_description = None
    schema_fields = tuple()

    if "type" not in property_ and "anyOf" in property_:
        for types in property_["anyOf"]:
            if types["type"] == "null":
                schema_mode = "NULLABLE"
            else:
                property_ = types

    try:
        schema_type, schema_mode = _get_schema_type_mode(property_, numeric_type, integer_type)
    except Exception as e:
        logger.error(f"While parsing {key}")
        raise
    if schema_type == "RECORD":
        schema_fields = tuple(parse_schema(property_, numeric_type, integer_type))

    if schema_mode == "REPEATED":
        # get child type
        try:
            schema_type, _ = _get_schema_type_mode(property_.get("items"),
                    numeric_type, integer_type)
        except Exception as e:
            logger.error(f"While parsing {key}")
            raise
        if schema_type == "RECORD":
            schema_fields = tuple(parse_schema(property_.get("items"),
                                               numeric_type, integer_type))

    return (schema_name, schema_type, schema_mode, schema_description,
            schema_fields)


def parse_schema(schema, numeric_type="NUMERIC", integer_type="INTEGER"):
    bq_schema = []
    for key in schema.get("properties", {}).keys():
        try:
            (schema_name, schema_type, schema_mode, schema_description,
             schema_fields) = _parse_property(key, schema["properties"][key],
                                              numeric_type, integer_type)
        except Exception as e:
            logger.error(f"{str(e)} at {schema['properties'][key]}")
            raise e
        schema_field = SchemaField(
            name=schema_name,
            field_type=schema_type,
            mode=schema_mode,
            description=schema_description,
            fields=schema_fields,
        )
        bq_schema.append(schema_field)

    if not bq_schema:
        logger.warn("RECORD type does not have properties." +
                    " Inserting a dummy string object")
        return parse_schema({"properties": {
            "dummy": {"type": ["null", "string"]}}},
            numeric_type, integer_type)

    return bq_schema


def modify_schema(
    config,
    catalog_file,
    streams=None,
    numeric_type="NUMERIC",
    integer_type="INTEGER",
    dryrun=False,
    continue_on_incompatible=False,
    ):
    with open(catalog_file, "r") as f:
        catalog = json.load(f)
    if isinstance(streams, str):
        streams = streams.split(",")
    client = Client(project=config["project_id"])

    col_map = config.get("column_map", {})

    table_prefix=config.get("table_prefix", "")
    table_ext=config.get("table_ext", "")

    incompatibles = {}
    for stream in catalog["streams"]:
        if not streams or stream["stream"] in streams:
            logger.info(f"Checking {stream['stream']}")
            schema = stream["schema"]
            bq_schema = parse_schema(schema)
            table_path = config["project_id"] + "." + config["dataset_id"] + "." + table_prefix + stream["stream"] + table_ext
            table = None
            try:
                table = client.get_table(table_path)
            except:
                pass
            if not table:
                logger.warning(f"Table does not exist: {table_path}. target-bigquery will create new table upon sync.")
                continue
            original_schema = table.schema
            new_schema = original_schema[:]   # Make a copy
            original_schema_dict = {}
            for sfield in original_schema:
                original_schema_dict[sfield.name] = sfield

            stream_col_map = col_map.get(stream["stream"], {})
            new_cols = 0
            incompatible_list = incompatibles.get(stream["stream"], [])
            for key in schema["properties"].keys():
                mapped_key = stream_col_map.get(key, key)

                # Note: When parsing, we need to use the original key,
                # but when instantiating a new schema, we need to use a mapped key as name
                (schema_name, schema_type, schema_mode, schema_description,
                 schema_fields) = _parse_property(key, schema["properties"][key],
                                                  numeric_type, integer_type)
                schema_name = mapped_key
                schema_field = SchemaField(schema_name, schema_type, schema_mode,
                                           schema_description, schema_fields)

                original_schema = original_schema_dict.get(mapped_key)
                if original_schema:
                    if str(original_schema) != str(schema_field):
                        incompatible_list.append(
                            f"  Column {mapped_key}: original and new have different schema.\nOrginal:\n   {str(original_schema)}\n...vs New:\n   {str(schema_field)}"
                        )
                    continue

                new_cols += 1
                logger.info(f"Adding Column {mapped_key} in Table {table_path}")
                new_schema.append(schema_field)

            for key in original_schema_dict.keys():
                if key not in schema["properties"].keys():
                    logger.warning(f"{key} not in the new schema any more!")

            if incompatible_list:
                msg = "Found incompatible types to the existing fields \n"
                if continue_on_incompatible:
                    msg += "...this change will be ignored with no change in '--continue-on-incompatible' mode.\n"
                msg += "\n".join(incompatible_list)

                if continue_on_incompatible:
                    logger.warning(msg)
                else:
                    logger.error(msg)
                    continue

            table.schema = new_schema
            if new_cols > 0:
                if not dryrun:
                    table = client.update_table(table, ["schema"])  # Make an API request.
                    logger.info(f"New columns have been added for {table_path}.")
                else:
                    logger.info(f"Dry-run: Not updating the columns for {table_path}")
            else:
                logger.info("No schema change detected!")


def remap_cols(data, mapper):
    if not mapper:
        return data

    new_data = dict(data)
    for key in data.keys():
        if key not in mapper.keys():
            continue
        new_data[mapper[key]] = data[key]
        new_data.pop(key)

    return new_data


format_checker = FormatChecker()
@format_checker.checks("date-time")
def check_datetime(value):
    return value is None or isinstance(value, str) and int(value[0:4]) >= 1970


def clean_and_validate(message, schemas, exclude_unknown_cols=False):
    batch_tstamp = datetime.datetime.utcnow()
    batch_tstamp = batch_tstamp.replace(
        tzinfo=datetime.timezone.utc)

    if message.stream not in schemas:
        raise Exception(("A record for stream {} was encountered" +
                         "before a corresponding schema").format(
                             message.stream))

    schema = schemas[message.stream]

    validation = {
        "is_valid": True
    }
    try:
        validate(instance=message.record, schema=schema, format_checker=format_checker)
    except ValidationError as e:
        validation["is_valid"] = False
        error_message = str(e)

        # It's a bit hacky and fragile here...
        instance = re.sub(r".*instance\[\'(.*)\'\].*", r"\1",
                          error_message.split("\n")[5])
        type_ = re.sub(r".*\{\'type\'\: \[\'.*\', \'(.*)\'\]\}.*",
                       r"\1", error_message.split("\n")[3])

        # Save number-convertible strings...
        if type_ in ["integer", "number"]:
            n = None
            try:
                n = float(message.record[instance])
            except Exception:
                # nullify in case we want to persist the rows with partially
                # invalid value with "force" mode:
                message.record[instance] = None
                pass
            if n is not None:
                validation["is_valid"] = True

        if validation["is_valid"] is False:
            validation["type"] = type_
            validation["instance"] = instance
            validation["record"] = message.record
            validation["message"] = str(e)

    if BATCH_TIMESTAMP in schema["properties"].keys():
        message.record[BATCH_TIMESTAMP] = batch_tstamp.isoformat()

    record = message.record

    # Before returning output, check for unknown columns
    cleaned_record = {}
    unknown_cols = []
    for key in record.keys():
        if key not in schema["properties"].keys():
            unknown_cols.append(key)
            continue
        cleaned_record[key] = record[key]
    if unknown_cols:
        msg = f"Unknown columns detected: {','.join(unknown_cols)}"
        if exclude_unknown_cols:
            validation["warning"] = msg
        elif validation["is_valid"]:
            validation["is_valid"] = False
            validation["type"] = ""
            validation["instance"] = ""
            validation["record"] = message.record
            validation["message"] = msg

    return cleaned_record, validation
