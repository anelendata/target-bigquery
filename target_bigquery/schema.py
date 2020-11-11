import datetime
import simplejson as json
from google.cloud.bigquery import SchemaField
from jsonschema import validate
from jsonschema.exceptions import ValidationError
import singer

# StitchData compatible timestamp meta data
#  https://www.stitchdata.com/docs/data-structure/system-tables-and-columns
BATCH_TIMESTAMP = "_sdc_batched_at"

logger = singer.get_logger()


def _parse_property(key, property_, numeric_type="NUMERIC"):
    schema_name = key

    schema_type = "STRING"
    schema_mode = "NULLABLE"
    schema_description = None
    schema_fields = tuple()

    if numeric_type not in ["NUMERIC", "FLOAT64"]:
        raise ValueError("Unknown numeric type %s" % numeric_type)

    if "type" not in property_ and "anyOf" in property_:
        for types in property_["anyOf"]:
            if types["type"] == "null":
                schema_mode = "NULLABLE"
            else:
                property_ = types

    if isinstance(property_["type"], list):
        if property_["type"][0] == "null":
            schema_mode = "NULLABLE"
        else:
            schema_mode = "required"
        schema_type = property_["type"][1]
    else:
        schema_type = property_["type"]

    if schema_type == "object":
        schema_type = "RECORD"
        schema_fields = tuple(parse_schema(property_, numeric_type))

    if schema_type == "array":
        schema_type = property_.get("items").get("type")[1]
        schema_mode = "REPEATED"
        if schema_type == "object":
            schema_type = "STRUCT"
            schema_fields = tuple(parse_schema(property_.get("items"),
                                               numeric_type))

    if schema_type == "string":
        if "format" in property_:
            if property_["format"] == "date-time":
                schema_type = "TIMESTAMP"

    if schema_type == "integer":
        schema_type = "INT64"

    if schema_type == "number":
        schema_type = numeric_type

    if schema_type == "boolean":
        schema_type = "BOOL"

    return (schema_name, schema_type, schema_mode, schema_description,
            schema_fields)


def parse_schema(schema, numeric_type="NUMERIC"):
    bq_schema = []
    for key in schema["properties"].keys():
        (schema_name, schema_type, schema_mode, schema_description,
         schema_fields) = _parse_property(key, schema["properties"][key],
                                          numeric_type)

        bq_schema.append(SchemaField(schema_name, schema_type, schema_mode,
                                     schema_description, schema_fields))

    return bq_schema


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

        # It's a big hacy and fragile here...
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
                # In case we want to persist the rows with partially
                # invalid value
                message.record[instance] = None
                pass
            if n is not None:
                cur_validation = True

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
        #         cur_validation = True

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
        try:
            record = bytes(json.dumps(record) + "\n", "UTF-8")
        except TypeError as e:
            logger.warning(record)
            raise

    return record, invalids
