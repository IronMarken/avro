package org.apache.avro.file;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.avro.utils.Utils.*;

@RunWith(value = Parameterized.class)
public class FileSerializationTest {

  private final String NOT_VALID_SCHEMA_PATH = "./src/test/resources/notValidSchema.avsc";
  private final String NOT_EXISTING_SCHEMA_PATH = "./src/test/resources/notValidSchema.avsc";
  private final String PRIMITIVE_SCHEMA_PATH = "./src/test/resources/primitiveType.avsc";
  private final String COMPLEX_SCHEMA_PATH = "./src/test/resources/complexType.avsc";
  private final String SERIALIZATION_FILE_PATH = "./src/test/resources/serialization.avro";
  private final String SERIALIZED_VALUE = "test";
  private final String RECORD_KEY = "value";
  private String actualSchemaPath;
  private Schema schema;
  private SchemaStatus schemaStatus;
  private File schemaFile;
  private File serializationFile;
  private boolean noDataSerialized;
  private boolean expectedException;

  @Parameterized.Parameters
  public static Collection<Object[]> testParameters() {
    return Arrays.asList(new Object[][] {
        // schemaType noDataSerialized expectedException
        { SchemaStatus.PRIMITIVE_SCHEMA, false, false }, { SchemaStatus.PRIMITIVE_SCHEMA, true, true },
        { SchemaStatus.COMPLEX_SCHEMA, false, false }, { SchemaStatus.COMPLEX_SCHEMA, true, true },
        { SchemaStatus.NOT_EXISTING_SCHEMA, false, true }, { SchemaStatus.NOT_EXISTING_SCHEMA, true, true },
        { SchemaStatus.NOT_VALID_SCHEMA, false, true }, { SchemaStatus.NOT_VALID_SCHEMA, true, true } });
  }

  public FileSerializationTest(SchemaStatus schemaStatus, boolean noDataSerialized, boolean expectedException) {
    this.configurePaths(schemaStatus);
    this.expectedException = expectedException;
    this.schemaStatus = schemaStatus;
    this.noDataSerialized = noDataSerialized;
  }

  private void configurePaths(SchemaStatus schemaStatus) {
    switch (schemaStatus) {
    case NOT_EXISTING_SCHEMA:
      this.actualSchemaPath = NOT_EXISTING_SCHEMA_PATH;
      break;
    case NOT_VALID_SCHEMA:
      this.actualSchemaPath = NOT_VALID_SCHEMA_PATH;
      break;
    case COMPLEX_SCHEMA:
      this.actualSchemaPath = COMPLEX_SCHEMA_PATH;
      break;
    case PRIMITIVE_SCHEMA:
    default:
      this.actualSchemaPath = PRIMITIVE_SCHEMA_PATH;
      break;
    }
  }

  @Before
  public void setupSchema() {
    try {
      // output schema file
      this.serializationFile = new File(SERIALIZATION_FILE_PATH);

    } catch (Exception e) {
      Assert.fail();
    }
  }

  private void serializePrimitiveData() throws Exception {
    DatumWriter<Utf8> datumWriter = new GenericDatumWriter<>(this.schema);
    DataFileWriter<Utf8> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(this.schema, this.serializationFile);
    dataFileWriter.append(new Utf8(SERIALIZED_VALUE));
    dataFileWriter.close();

  }

  private void serializeComplexData() throws Exception {
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(this.schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(this.schema, this.serializationFile);
    GenericRecord record = new GenericData.Record(this.schema);
    record.put(RECORD_KEY, SERIALIZED_VALUE);
    dataFileWriter.append(record);
    dataFileWriter.close();

  }

  private Utf8 deserializePrimitiveData() throws Exception {
    DatumReader<Utf8> datumReader = new GenericDatumReader<>(this.schema);
    DataFileReader<Utf8> dataFileReader = new DataFileReader<>(this.serializationFile, datumReader);
    return dataFileReader.next();
  }

  private Utf8 deserializeComplexData() throws Exception {
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(this.schema);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(this.serializationFile, datumReader);
    GenericRecord recordRead = dataFileReader.next();
    return (Utf8) recordRead.get(RECORD_KEY);
  }

  @Test
  public void serializationTest() {
    boolean actualException = false;
    try {
      // create schema file
      this.schemaFile = new File(this.actualSchemaPath);

      // parse schema
      this.schema = new Schema.Parser().parse(this.schemaFile);

      if (!this.noDataSerialized) {
        // serialize data
        switch (this.schemaStatus) {
        case COMPLEX_SCHEMA:
          serializeComplexData();
          break;
        case PRIMITIVE_SCHEMA:
        default:
          serializePrimitiveData();
          break;
        }
      }

      Utf8 actualValue;
      Utf8 expectedValue = new Utf8(SERIALIZED_VALUE);

      // deserialize data
      switch (this.schemaStatus) {
      case COMPLEX_SCHEMA:
        actualValue = deserializeComplexData();
        break;
      case PRIMITIVE_SCHEMA:
      default:
        actualValue = deserializePrimitiveData();
        break;
      }

      // Assert value
      Assert.assertEquals(expectedValue, actualValue);

    } catch (Exception e) {
      actualException = true;
    }

    // Assert exception
    Assert.assertEquals(expectedException, actualException);

  }

  @After
  public void clearFile() {
    if (this.serializationFile.exists()) {
      if (!this.serializationFile.delete())
        Assert.fail();
    }
  }

}
