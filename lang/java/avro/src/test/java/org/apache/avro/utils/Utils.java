package org.apache.avro.utils;

public class Utils {

  public enum SchemaStatus {
    NOT_EXISTING_SCHEMA, NOT_VALID_SCHEMA, RECORD_SCHEMA, MAP_SCHEMA, ARRAY_SCHEMA, FIXED_SCHEMA, PRIMITIVE_SCHEMA
  }

  public enum InsertOptions {
    NO_DATA, SINGLE_VALUE, MULTI_VALUES
  }
}
