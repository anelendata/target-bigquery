import datetime
import simplejson as json
import re

from google.cloud.bigquery import SchemaField
from jsonschema import validate
from jsonschema.exceptions import ValidationError
import singer


# StitchData compatible timestamp meta data
#  https://www.stitchdata.com/docs/data-structure/system-tables-and-columns
BATCH_TIMESTAMP = "_sdc_batched_at"
JSONSCHEMA_TYPES = ["object", "array", "string", "integer", "number", "boolean"]

logger = singer.get_logger()


def _get_schema_type_mode(property_, numeric_type):
    type_ = property_.get("type")

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
        raise Exception(f"{jsonschema_type} is not a valid jsonschema type")

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

    if jsonschema_type == "integer":
        schema_type = "INT64"

    if jsonschema_type == "number":
        schema_type = numeric_type

    if jsonschema_type == "boolean":
        schema_type = "BOOL"

    return schema_type, schema_mode


def _parse_property(key, property_, numeric_type="NUMERIC"):
    if numeric_type not in ["NUMERIC", "FLOAT64"]:
        raise ValueError("Unknown numeric type %s" % numeric_type)

    schema_name = key
    schema_description = None
    schema_fields = tuple()

    if "type" not in property_ and "anyOf" in property_:
        for types in property_["anyOf"]:
            if types["type"] == "null":
                schema_mode = "NULLABLE"
            else:
                property_ = types

    schema_type, schema_mode = _get_schema_type_mode(property_, numeric_type)

    if schema_type == "RECORD":
        schema_fields = tuple(parse_schema(property_, numeric_type))

    if schema_mode == "REPEATED":
        # get child type
        schema_type, _ = _get_schema_type_mode(property_.get("items"),
                                               numeric_type)

        if schema_type == "RECORD":
            schema_fields = tuple(parse_schema(property_.get("items"),
                                               numeric_type))

    return (schema_name, schema_type, schema_mode, schema_description,
            schema_fields)


def parse_schema(schema, numeric_type="NUMERIC"):
    bq_schema = []
    for key in schema.get("properties", {}).keys():
        (schema_name, schema_type, schema_mode, schema_description,
         schema_fields) = _parse_property(key, schema["properties"][key],
                                          numeric_type)
        schema_field = SchemaField(schema_name, schema_type, schema_mode,
                                   schema_description, schema_fields)
        bq_schema.append(schema_field)

    if not bq_schema:
        logger.warn("RECORD type does not have properties." +
                    " Inserting a dummy string object")
        return parse_schema({"properties": {
            "dummy": {"type": ["null", "string"]}}},
            numeric_type)

    return bq_schema


def clean_and_validate(message, schemas, json_dumps=False):
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
        validate(message.record, schema)
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

        # TODO:
        # Convert to BigQuery timestamp type (iso 8601)
        # if type_ == "string" and format_ == "date-time":
        #     n = None
        #     try:
        #         n = float(message.record[instance])
        #         d = datetime.datetime.fromtimestamp(n)
        #         d = d.replace(tzinfo=datetime.timezone.utc)
        #         message.record[instance] = d.isoformat()
        #     except Exception:
        #         # In case we want to persist the rows with partially
        #         # invalid value
        #         message.record[instance] = None
        #         pass
        #     if d is not None:
        #         validation["is_valid"] = True

        if validation["is_valid"] is False:
            validation["type"] = type_
            validation["instance"] = instance
            validation["record"] = message.record
            validation["message"] = str(e)

    if BATCH_TIMESTAMP in schema["properties"].keys():
        message.record[BATCH_TIMESTAMP] = batch_tstamp.isoformat()

    record = message.record
    if json_dumps:
        try:
            record = bytes(json.dumps(record) + "\n", "UTF-8")
        except TypeError as e:
            logger.warning(record)
            raise

    return record, validation
