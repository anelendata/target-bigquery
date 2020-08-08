from google.cloud.bigquery import SchemaField


def _parse_property(key, property_):
    schema_name = key

    schema_type = "STRING"
    schema_mode = "NULLABLE"
    schema_description = None
    schema_fields = tuple()

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
        schema_fields = tuple(parse_schema(property_))

    if schema_type == "array":
        schema_type = property_.get("items").get("type")[1]
        schema_mode = "ARRAY"
        if schema_type == "object":
            schema_type = "STRUCT"
            schema_fields = tuple(parse_schema(property_.get("items")))

    if schema_type == "string":
        if "format" in property_:
            if property_["format"] == "date-time":
                schema_type = "TIMESTAMP"

    if schema_type == "integer":
        schema_type = "INT64"

    if schema_type == "number":
        schema_type = "NUMERIC"

    if schema_type == "boolean":
        schema_type = "BOOL"

    return (schema_name, schema_type, schema_mode, schema_description,
            schema_fields)


def parse_schema(schema):
    bq_schema = []
    for key in schema["properties"].keys():
        (schema_name, schema_type, schema_mode, schema_description,
         schema_fields) = _parse_property(key, schema["properties"][key])

        bq_schema.append(SchemaField(schema_name, schema_type, schema_mode,
                                     schema_description, schema_fields))

    return bq_schema
