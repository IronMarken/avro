package org.apache.avro.file;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.avro.utils.Utils.*;
import static org.apache.avro.utils.Utils.InsertOptions.*;

@RunWith(value = Parameterized.class)
public class FileSerializationTest {

  private final String NOT_VALID_SCHEMA_PATH = "./src/test/resources/notValidSchema.avsc";
  private final String NOT_EXISTING_SCHEMA_PATH = "./src/test/resources/notExisting.avsc";
  private final String PRIMITIVE_SCHEMA_PATH = "./src/test/resources/primitiveType.avsc";
  private final String RECORD_SCHEMA_PATH = "./src/test/resources/recordComplexType.avsc";
  private final String MAP_SCHEMA_PATH = "./src/test/resources/mapComplexType.avsc";
  private final String ARRAY_SCHEMA_PATH = "./src/test/resources/arrayComplexType.avsc";
  private final String FIXED_SCHEMA_PATH = "./src/test/resources/fixedComplexType.avsc";
  private final String SERIALIZATION_FILE_PATH = "./src/test/resources/serialization.avro";
  private final String SERIALIZED_VALUE = "test";
  private final String FIELD_NAME = "value";
  private final String KEY = "key";
  private int MAX_INSERT = 1000;
  private String actualSchemaPath;
  private Schema schema;
  private SchemaStatus schemaStatus;
  private File schemaFile;
  private File serializationFile;
  private InsertOptions insertOption;
  private boolean expectedException;
  private int expectedSize;

  @Parameterized.Parameters
  public static Collection<Object[]> testParameters() {
    return Arrays.asList(new Object[][] {
        // schemaType insertOption expectedException
        { SchemaStatus.PRIMITIVE_SCHEMA, MULTI_VALUES, false }, { SchemaStatus.PRIMITIVE_SCHEMA, SINGLE_VALUE, false },
        { SchemaStatus.PRIMITIVE_SCHEMA, NO_DATA, true }, { SchemaStatus.RECORD_SCHEMA, MULTI_VALUES, false },
        { SchemaStatus.RECORD_SCHEMA, SINGLE_VALUE, false }, { SchemaStatus.RECORD_SCHEMA, NO_DATA, true },
        { SchemaStatus.FIXED_SCHEMA, MULTI_VALUES, false }, { SchemaStatus.FIXED_SCHEMA, SINGLE_VALUE, false },
        { SchemaStatus.FIXED_SCHEMA, NO_DATA, true }, { SchemaStatus.ARRAY_SCHEMA, MULTI_VALUES, false },
        { SchemaStatus.ARRAY_SCHEMA, SINGLE_VALUE, false }, { SchemaStatus.ARRAY_SCHEMA, NO_DATA, true },
        { SchemaStatus.MAP_SCHEMA, MULTI_VALUES, false }, { SchemaStatus.MAP_SCHEMA, SINGLE_VALUE, false },
        { SchemaStatus.MAP_SCHEMA, NO_DATA, true }, { SchemaStatus.NOT_EXISTING_SCHEMA, MULTI_VALUES, true },
        { SchemaStatus.NOT_EXISTING_SCHEMA, SINGLE_VALUE, true }, { SchemaStatus.NOT_EXISTING_SCHEMA, NO_DATA, true },
        { SchemaStatus.NOT_VALID_SCHEMA, MULTI_VALUES, true }, { SchemaStatus.NOT_VALID_SCHEMA, SINGLE_VALUE, true },
        { SchemaStatus.NOT_VALID_SCHEMA, NO_DATA, true } });
  }

  public FileSerializationTest(SchemaStatus schemaStatus, InsertOptions insertOption, boolean expectedException) {
    this.configurePaths(schemaStatus);
    this.expectedException = expectedException;
    this.schemaStatus = schemaStatus;
    this.insertOption = insertOption;
  }

  private void configurePaths(SchemaStatus schemaStatus) {
    switch (schemaStatus) {
    case NOT_EXISTING_SCHEMA:
      this.actualSchemaPath = NOT_EXISTING_SCHEMA_PATH;
      break;
    case NOT_VALID_SCHEMA:
      this.actualSchemaPath = NOT_VALID_SCHEMA_PATH;
      break;
    case RECORD_SCHEMA:
      this.actualSchemaPath = RECORD_SCHEMA_PATH;
      break;
    case MAP_SCHEMA:
      this.actualSchemaPath = MAP_SCHEMA_PATH;
      break;
    case ARRAY_SCHEMA:
      this.actualSchemaPath = ARRAY_SCHEMA_PATH;
      break;
    case FIXED_SCHEMA:
      this.actualSchemaPath = FIXED_SCHEMA_PATH;
      break;
    // default value
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

      // setup number of insert
      switch (this.insertOption) {
      case NO_DATA:
        this.expectedSize = 0;
        break;
      case MULTI_VALUES:
        Random random = new Random();
        // MIN 1 repetition
        this.expectedSize = random.nextInt(MAX_INSERT - 1) + 1;
        break;
      // default value
      case SINGLE_VALUE:
      default:
        this.expectedSize = 1;
        break;
      }
    } catch (Exception e) {
      Assert.fail();
    }
  }

  private void serializePrimitiveData() throws Exception {
    DatumWriter<Utf8> datumWriter = new GenericDatumWriter<>(this.schema);
    DataFileWriter<Utf8> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(this.schema, this.serializationFile);
    for (int i = 0; i < this.expectedSize; i++) {
      dataFileWriter.append(new Utf8(SERIALIZED_VALUE + "-" + (i + 1)));
    }
    dataFileWriter.close();

  }

  private List<Utf8> deserializePrimitiveData() throws Exception {
    DatumReader<Utf8> datumReader = new GenericDatumReader<>(this.schema);
    DataFileReader<Utf8> dataFileReader = new DataFileReader<>(this.serializationFile, datumReader);
    List<Utf8> returnList = new ArrayList<>();

    while (dataFileReader.hasNext()) {
      returnList.add(dataFileReader.next());
    }
    return returnList;
  }

  private void serializeRecordData() throws Exception {
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(this.schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(this.schema, this.serializationFile);
    for (int i = 0; i < this.expectedSize; i++) {
      GenericRecord record = new GenericData.Record(this.schema);
      record.put(FIELD_NAME, new Utf8(SERIALIZED_VALUE + "-" + (i + 1)));
      dataFileWriter.append(record);
    }
    dataFileWriter.close();
  }

  private List<Utf8> deserializeRecordData() throws Exception {
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(this.schema);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(this.serializationFile, datumReader);
    List<Utf8> returnList = new ArrayList<>();

    while (dataFileReader.hasNext()) {
      GenericRecord recordRead = dataFileReader.next();
      returnList.add((Utf8) recordRead.get(FIELD_NAME));
    }
    return returnList;
  }

  private void serializeFixedData() throws Exception {
    int fixedSize = this.schema.getFixedSize();
    DatumWriter<GenericFixed> datumWriter = new GenericDatumWriter<>(this.schema);
    DataFileWriter<GenericFixed> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(this.schema, this.serializationFile);

    for (int i = 0; i < this.expectedSize; i++) {
      StringBuilder valueBuilder = new StringBuilder(SERIALIZED_VALUE + "-" + (i + 1));
      int stringLength = valueBuilder.length();
      int lengthDiff = fixedSize - stringLength;
      if (lengthDiff > 0) {
        for (int k = 0; k < lengthDiff; k++) {
          valueBuilder.append("a");
        }
      }
      String value = valueBuilder.toString();
      byte[] rawBytes = value.getBytes();
      GenericFixed genericFixed = new GenericData.Fixed(this.schema, rawBytes);
      dataFileWriter.append(genericFixed);
    }
    dataFileWriter.close();
  }

  private List<Utf8> deserializeFixedData() throws Exception {
    List<Utf8> returnList = new ArrayList<>();
    DatumReader<GenericFixed> datumReader = new GenericDatumReader<>(this.schema);
    DataFileReader<GenericFixed> dataFileReader = new DataFileReader<>(this.serializationFile, datumReader);

    while (dataFileReader.hasNext()) {
      GenericFixed recordRead = dataFileReader.next();
      returnList.add(new Utf8(recordRead.bytes()));
    }
    return returnList;
  }

  private void serializeArrayData() throws Exception {
    DatumWriter<List<Utf8>> datumWriter = new GenericDatumWriter<>(this.schema);
    DataFileWriter<List<Utf8>> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(this.schema, this.serializationFile);

    // create n arrays of dimension n
    // where n is this.expectedSize
    for (int arrayIndex = 0; arrayIndex < this.expectedSize; arrayIndex++) {
      List<Utf8> serializeArray = new ArrayList<>();
      for (int elementIndex = 0; elementIndex < this.expectedSize; elementIndex++) {
        String value = SERIALIZED_VALUE + "-" + (arrayIndex + 1) + "-" + (elementIndex + 1);
        Utf8 utf8String = new Utf8(value);
        serializeArray.add(utf8String);
      }
      dataFileWriter.append(serializeArray);
    }

    dataFileWriter.close();
  }

  private List<List<Utf8>> deserializeArrayData() throws Exception {
    DatumReader<List<Utf8>> datumReader = new GenericDatumReader<>(this.schema);
    DataFileReader<List<Utf8>> dataFileReader = new DataFileReader<>(this.serializationFile, datumReader);

    List<List<Utf8>> returnList = new ArrayList<>();

    while (dataFileReader.hasNext()) {
      List<Utf8> arrayRead = dataFileReader.next();
      returnList.add(arrayRead);
    }

    return returnList;
  }

  private void serializeMapData() throws Exception {
    DatumWriter<Map<String, Utf8>> datumWriter = new GenericDatumWriter<>(this.schema);
    DataFileWriter<Map<String, Utf8>> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(this.schema, this.serializationFile);

    for (int mapIndex = 0; mapIndex < this.expectedSize; mapIndex++) {
      Map<String, Utf8> utf8Map = new HashMap<>();
      for (int keyIndex = 0; keyIndex < this.expectedSize; keyIndex++) {
        String value = SERIALIZED_VALUE + "-" + (mapIndex + 1) + "-" + (keyIndex + 1);
        String key = KEY + "-" + (mapIndex + 1) + "-" + (keyIndex + 1);
        Utf8 utf8Value = new Utf8(value);

        utf8Map.put(key, utf8Value);
      }
      dataFileWriter.append(utf8Map);
    }

    dataFileWriter.close();
  }

  // for the serialization needed String type key
  // during the deserialization the key become Utf8 type
  // because AVRO serialize String type as Utf8 type
  private List<Map<Utf8, Utf8>> deserializeMapData() throws Exception {
    DatumReader<Map<Utf8, Utf8>> datumReader = new GenericDatumReader<>(this.schema);
    DataFileReader<Map<Utf8, Utf8>> dataFileReader = new DataFileReader<>(this.serializationFile, datumReader);

    List<Map<Utf8, Utf8>> returnList = new ArrayList<>();

    while (dataFileReader.hasNext()) {
      Map<Utf8, Utf8> mapRead = dataFileReader.next();
      returnList.add(mapRead);
    }

    return returnList;
  }

  @Test
  public void serializationTest() {
    boolean actualException = false;
    try {
      // create schema file
      this.schemaFile = new File(this.actualSchemaPath);

      // parse schema
      this.schema = new Schema.Parser().parse(this.schemaFile);

      if (this.expectedSize != 0) {
        // serialize data
        switch (this.schemaStatus) {
        case MAP_SCHEMA:
          serializeMapData();
          break;
        case ARRAY_SCHEMA:
          serializeArrayData();
          break;
        case FIXED_SCHEMA:
          serializeFixedData();
          break;
        case RECORD_SCHEMA:
          serializeRecordData();
          break;
        case PRIMITIVE_SCHEMA:
        default:
          serializePrimitiveData();
          break;
        }
      }

      List<Utf8> deserializedList;
      List<Map<Utf8, Utf8>> deserializedMapList;
      Utf8 expectedValue;
      Utf8 actualValue;

      List<List<Utf8>> deserializedMultiList;
      int actualSize;
      boolean multiList;
      boolean mapList;

      // deserialize data
      switch (this.schemaStatus) {
      case MAP_SCHEMA:
        deserializedMapList = deserializeMapData();
        deserializedMultiList = null;
        deserializedList = null;
        actualSize = deserializedMapList.size();
        multiList = false;
        mapList = true;
        break;
      case ARRAY_SCHEMA:
        deserializedMapList = null;
        deserializedMultiList = deserializeArrayData();
        deserializedList = null;
        actualSize = deserializedMultiList.size();
        multiList = true;
        mapList = false;
        break;
      case FIXED_SCHEMA:
        deserializedMapList = null;
        deserializedList = deserializeFixedData();
        deserializedMultiList = null;
        actualSize = deserializedList.size();
        multiList = false;
        mapList = false;
        break;
      case RECORD_SCHEMA:
        deserializedMapList = null;
        deserializedList = deserializeRecordData();
        deserializedMultiList = null;
        actualSize = deserializedList.size();
        multiList = false;
        mapList = false;
        break;
      case PRIMITIVE_SCHEMA:
      default:
        deserializedMapList = null;
        deserializedList = deserializePrimitiveData();
        deserializedMultiList = null;
        actualSize = deserializedList.size();
        multiList = false;
        mapList = false;
        break;
      }

      // assert on size
      Assert.assertEquals(this.expectedSize, actualSize);

      // manage List or List of List
      if (multiList) {
        for (int arrayIndex = 0; arrayIndex < this.expectedSize; arrayIndex++) {
          List<Utf8> utf8List = deserializedMultiList.get(arrayIndex);
          actualSize = deserializedMultiList.size();

          // Assert dimension of each list
          Assert.assertEquals(this.expectedSize, actualSize);

          // check single element
          for (int elementIndex = 0; elementIndex < this.expectedSize; elementIndex++) {
            expectedValue = new Utf8(SERIALIZED_VALUE + "-" + (arrayIndex + 1) + "-" + (elementIndex + 1));
            actualValue = utf8List.get(elementIndex);

            Assert.assertEquals(expectedValue, actualValue);

          }
        }
      } else {
        // single list case

        if (mapList) {
          // map list case
          for (int mapIndex = 0; mapIndex < this.expectedSize; mapIndex++) {
            Map<Utf8, Utf8> mapUtf8 = deserializedMapList.get(mapIndex);

            actualSize = mapUtf8.size();
            // assert on map size
            Assert.assertEquals(this.expectedSize, actualSize);

            // check all map key and value
            for (int keyIndex = 0; keyIndex < this.expectedSize; keyIndex++) {
              String key = KEY + "-" + (mapIndex + 1) + "-" + (keyIndex + 1);
              String value = SERIALIZED_VALUE + "-" + (mapIndex + 1) + "-" + (keyIndex + 1);
              Utf8 utf8Key = new Utf8(key);

              boolean isKeyPresent = mapUtf8.containsKey(utf8Key);

              // Assert key is present
              Assert.assertTrue(isKeyPresent);

              // get value
              actualValue = mapUtf8.get(utf8Key);
              expectedValue = new Utf8(value);

              // Assert on value
              Assert.assertEquals(expectedValue, actualValue);
            }
          }
        } else {
          for (int i = 0; i < this.expectedSize; i++) {

            StringBuilder strValue = new StringBuilder(SERIALIZED_VALUE + "-" + (i + 1));

            // modify expected string for fixed length
            if (this.schemaStatus == SchemaStatus.FIXED_SCHEMA) {
              int fixedSize = this.schema.getFixedSize();
              int stringLength = strValue.length();
              int lengthDiff = fixedSize - stringLength;
              if (lengthDiff > 0) {
                for (int k = 0; k < lengthDiff; k++) {
                  strValue.append("a");
                }
              }
            }

            expectedValue = new Utf8(strValue.toString());
            actualValue = deserializedList.get(i);

            // Assert value
            Assert.assertEquals(expectedValue, actualValue);
          }
        }
      }

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
