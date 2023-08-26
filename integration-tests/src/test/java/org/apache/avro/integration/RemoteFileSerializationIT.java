package org.apache.avro.integration;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.ipc.*;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.ipc.generic.GenericResponder;
import org.apache.avro.ipc.jetty.HttpServer;
import org.apache.avro.ipc.netty.NettyServer;
import org.apache.avro.ipc.netty.NettyTransceiver;
import org.apache.avro.util.Utf8;
import org.apache.avro.utils.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.*;

import static org.apache.avro.utils.Utils.ClientStatus.SINGLE_CLIENT;
import static org.apache.avro.utils.Utils.ClientStatus.MULTI_CLIENT;
import static org.apache.avro.utils.Utils.CommunicationStatus.*;
import static org.apache.avro.utils.Utils.DataTypeStatus.*;
import static org.apache.avro.utils.Utils.ProtocolStatus.*;

@RunWith(value = Parameterized.class)
public class RemoteFileSerializationIT {

  private final String VALID_PROTOCOL_PATH = "./src/test/resources/protocol.avpr";
  private final String NOT_VALID_PROTOCOL_PATH = "./src/test/resources/notValid.avpr";
  private final String NOT_EXISTING_PROTOCOL_PATH = "./src/test/resources/notExisting.avpr";
  private final String SERIALIZATION_FILE_PATH = "./src/test/resources/serialization.avro";
  private final String SERIALIZE_PRIMITIVE_MSG = "serialize_primitive";
  private final String DESERIALIZE_PRIMITIVE_MSG = "deserialize_primitive";
  private final String SERIALIZE_RECORD_MSG = "serialize_record";
  private final String DESERIALIZE_RECORD_MSG = "deserialize_record";
  private final String SERIALIZE_MAP_MSG = "serialize_map";
  private final String DESERIALIZE_MAP_MSG = "deserialize_map";
  private final String SERIALIZE_ARRAY_MSG = "serialize_array";
  private final String DESERIALIZE_ARRAY_MSG = "deserialize_array";
  private final String RECORD_TYPE_NAME = "CustomRecord";
  private final String REQUEST_KEY = "req";
  private final String RECORD_KEY = "value";
  private final String RECORD_VALUE = "recordValue";
  private final String RECORD_SERIALIZATION_ACK = "recordSerializationAck";
  private final String PRIMITIVE_VALUE = "primitiveValue";
  private final String PRIMITIVE_SERIALIZATION_ACK = "primitiveSerializationAck";
  private final String MAP_KEY = "mapKey";
  private final String MAP_VALUE = "mapValue";
  private final String MAP_SERIALIZATION_ACK = "mapSerializationAck";
  private final String ARRAY_VALUE = "arrayValue";
  private final String ARRAY_SERIALIZATION_ACK = "arraySerializationAck";
  private final String DESERIALIZATION_REQ = "deserializeReq";
  private final String HOST = "localhost";
  private final int PORT = 3001;
  private boolean expectedException;
  private String protocolPath;
  private Utils.CommunicationStatus communicationStatus;
  private boolean isMultiClient;
  private String serializeMsg;
  private String deserializeMsg;
  private Utils.DataTypeStatus dataTypeStatus;
  private File protoFile;
  private File serializationFile;
  private Server server;
  private Transceiver client1;
  private Transceiver client2;
  private Protocol protocol;


  @Parameterized.Parameters
  public static Collection<Object> testParameters() {
    return Arrays.asList(new Object[][]{
      //protocolStatus, communicationStatus, clientStatus, dataTypeStatus, expectedException
      {VALID_PROTOCOL, SOCKET_COMMUNICATION, SINGLE_CLIENT, PRIMITIVE_DATA, false},
      {VALID_PROTOCOL, HTTP_COMMUNICATION, SINGLE_CLIENT, PRIMITIVE_DATA, false},
      {VALID_PROTOCOL, NETTY_COMMUNICATION, SINGLE_CLIENT, PRIMITIVE_DATA, false},
      {VALID_PROTOCOL, SOCKET_COMMUNICATION, SINGLE_CLIENT, RECORD_DATA, false},
      {VALID_PROTOCOL, HTTP_COMMUNICATION, SINGLE_CLIENT, RECORD_DATA, false},
      {VALID_PROTOCOL, NETTY_COMMUNICATION, SINGLE_CLIENT, RECORD_DATA, false},
      {VALID_PROTOCOL, SOCKET_COMMUNICATION, SINGLE_CLIENT, MAP_DATA, false},
      {VALID_PROTOCOL, HTTP_COMMUNICATION, SINGLE_CLIENT, MAP_DATA, false},
      {VALID_PROTOCOL, NETTY_COMMUNICATION, SINGLE_CLIENT, MAP_DATA, false},
      {VALID_PROTOCOL, SOCKET_COMMUNICATION, SINGLE_CLIENT, ARRAY_DATA, false},
      {VALID_PROTOCOL, HTTP_COMMUNICATION, SINGLE_CLIENT, ARRAY_DATA, false},
      {VALID_PROTOCOL, NETTY_COMMUNICATION, SINGLE_CLIENT, ARRAY_DATA, false},
      {VALID_PROTOCOL, SOCKET_COMMUNICATION, MULTI_CLIENT, PRIMITIVE_DATA, false},
      {VALID_PROTOCOL, HTTP_COMMUNICATION, MULTI_CLIENT, PRIMITIVE_DATA, false},
      {VALID_PROTOCOL, NETTY_COMMUNICATION, MULTI_CLIENT, PRIMITIVE_DATA, false},
      {VALID_PROTOCOL, SOCKET_COMMUNICATION, MULTI_CLIENT, RECORD_DATA, false},
      {VALID_PROTOCOL, HTTP_COMMUNICATION, MULTI_CLIENT, RECORD_DATA, false},
      {VALID_PROTOCOL, NETTY_COMMUNICATION, MULTI_CLIENT, RECORD_DATA, false},
      {VALID_PROTOCOL, SOCKET_COMMUNICATION, MULTI_CLIENT, MAP_DATA, false},
      {VALID_PROTOCOL, HTTP_COMMUNICATION, MULTI_CLIENT, MAP_DATA, false},
      {VALID_PROTOCOL, NETTY_COMMUNICATION, MULTI_CLIENT, MAP_DATA, false},
      {VALID_PROTOCOL, SOCKET_COMMUNICATION, MULTI_CLIENT, ARRAY_DATA, false},
      {VALID_PROTOCOL, HTTP_COMMUNICATION, MULTI_CLIENT, ARRAY_DATA, false},
      {VALID_PROTOCOL, NETTY_COMMUNICATION, MULTI_CLIENT, ARRAY_DATA, false},
      {NOT_VALID_PROTOCOL, SOCKET_COMMUNICATION, SINGLE_CLIENT, PRIMITIVE_DATA, true},
      {NOT_EXISTING_PROTOCOL, SOCKET_COMMUNICATION, SINGLE_CLIENT, PRIMITIVE_DATA, true}
    });
  }


  public RemoteFileSerializationIT(Utils.ProtocolStatus protocolStatus, Utils.CommunicationStatus communicationStatus, Utils.ClientStatus clientStatus, Utils.DataTypeStatus dataTypeStatus, boolean expectedException) {
    this.expectedException = expectedException;
    this.communicationStatus = communicationStatus;
    this.configureClient(clientStatus);
    this.configureProtocolPath(protocolStatus);
    this.configureMsg(dataTypeStatus);

  }

  private void configureClient(Utils.ClientStatus clientStatus) {
    switch (clientStatus) {
      case MULTI_CLIENT:
        this.isMultiClient = true;
        break;
      case SINGLE_CLIENT:
      default:
        this.isMultiClient = false;
        break;
    }
  }

  private void configureMsg(Utils.DataTypeStatus dataTypeStatus) {
    this.dataTypeStatus = dataTypeStatus;
    switch (dataTypeStatus) {
      case ARRAY_DATA:
        this.serializeMsg = SERIALIZE_ARRAY_MSG;
        this.deserializeMsg = DESERIALIZE_ARRAY_MSG;
        break;
      case MAP_DATA:
        this.serializeMsg = SERIALIZE_MAP_MSG;
        this.deserializeMsg = DESERIALIZE_MAP_MSG;
        break;
      case RECORD_DATA:
        this.serializeMsg = SERIALIZE_RECORD_MSG;
        this.deserializeMsg = DESERIALIZE_RECORD_MSG;
        break;
      case PRIMITIVE_DATA:
      default:
        this.serializeMsg = SERIALIZE_PRIMITIVE_MSG;
        this.deserializeMsg = DESERIALIZE_PRIMITIVE_MSG;
        break;
    }
  }

  private void configureProtocolPath(Utils.ProtocolStatus protocolStatus) {
    switch (protocolStatus) {
      case NOT_EXISTING_PROTOCOL:
        this.protocolPath = NOT_EXISTING_PROTOCOL_PATH;
        break;
      case NOT_VALID_PROTOCOL:
        this.protocolPath = NOT_VALID_PROTOCOL_PATH;
        break;
      case VALID_PROTOCOL:
      default:
        this.protocolPath = VALID_PROTOCOL_PATH;
        break;
    }
  }

  @Before
  public void setupFiles() {
    this.protoFile = new File(this.protocolPath);
    this.serializationFile = new File(SERIALIZATION_FILE_PATH);
  }

  @After
  public void shutdownAndClearFile() {
    // delete file
    if (this.serializationFile.exists()) {
      if (!this.serializationFile.delete())
        Assert.fail();
    }

    // shutdown clients and server
    try {
      if (!this.expectedException) {
        this.server.close();
        this.client1.close();
        if (client2 != null)
          this.client2.close();
      }
    } catch (Exception e) {
      Assert.fail();
    }
  }

  private void serializeArrayData(List<Utf8> arrayData) throws Exception {
    // generate schema of array of string items
    Schema arraySchema = SchemaBuilder.array().items().stringType();

    // setup DatumWriter and FileWriter
    DatumWriter<List<Utf8>> datumWriter = new GenericDatumWriter<>(arraySchema);
    DataFileWriter<List<Utf8>> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(arraySchema, this.serializationFile);

    // serialize
    dataFileWriter.append(arrayData);
    dataFileWriter.close();
  }

  private void serializeMapData(Map<Utf8, Utf8> mapData) throws Exception {
    // generate schema of map<Utf8, Utf8>
    Schema mapSchema = SchemaBuilder.map().values().stringType();

    // setup DatumWriter and FileWriter
    DatumWriter<Map<Utf8, Utf8>> datumWriter = new GenericDatumWriter<>(mapSchema);
    DataFileWriter<Map<Utf8, Utf8>> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(mapSchema, this.serializationFile);

    // serialize
    dataFileWriter.append(mapData);
    dataFileWriter.close();
  }

  private void serializeRecordData(GenericRecord recordData) throws Exception {
    // generate schema
    Schema recordSchema = this.protocol.getType(RECORD_TYPE_NAME);

    // setup DatumWriter and FileWriter
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(recordSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(recordSchema, this.serializationFile);

    // serialize
    dataFileWriter.append(recordData);
    dataFileWriter.close();
  }

  private void serializePrimitiveData(Utf8 stringData) throws Exception {
    // generate schema
    Schema stringSchema = Schema.create(Schema.Type.STRING);

    // setup DatumWriter and FileWriter
    DatumWriter<Utf8> datumWriter = new GenericDatumWriter<>(stringSchema);
    DataFileWriter<Utf8> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(stringSchema, this.serializationFile);

    // serialize
    dataFileWriter.append(stringData);
    dataFileWriter.close();
  }

  private List<Utf8> deserializeArrayData() throws Exception {
    List<Utf8> returnList = new ArrayList<>();

    // generate schema
    Schema arraySchema = SchemaBuilder.array().items().stringType();

    // setup DatumReader and FileReader
    DatumReader<List<Utf8>> datumReader = new GenericDatumReader<>(arraySchema);
    DataFileReader<List<Utf8>> dataFileReader = new DataFileReader<>(this.serializationFile, datumReader);

    // deserialize
    if (dataFileReader.hasNext()) {
      // single array for construction
      returnList = dataFileReader.next();
    }

    return returnList;
  }

  private Map<Utf8, Utf8> deserializeMapData() throws Exception {
    Map<Utf8, Utf8> returnMap = new HashMap<>();

    // generate schema
    Schema mapSchema = SchemaBuilder.map().values().stringType();

    // setup DatumReader and FileReader
    DatumReader<Map<Utf8, Utf8>> datumReader = new GenericDatumReader<>(mapSchema);
    DataFileReader<Map<Utf8, Utf8>> dataFileReader = new DataFileReader<>(this.serializationFile, datumReader);

    //deserialize
    if (dataFileReader.hasNext()) {
      // single map for construction
      returnMap = dataFileReader.next();
    }

    return returnMap;
  }

  private GenericRecord deserializeRecordData() throws Exception {
    // generate Schema and initialize return value
    Schema recordSchema = this.protocol.getType(RECORD_TYPE_NAME);
    GenericRecord returnRecord = new GenericData.Record(recordSchema);


    // setup DatumReader and FileReader
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(recordSchema);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(this.serializationFile, datumReader);

    // deserialize
    if (dataFileReader.hasNext()) {
      // single record for construction
      returnRecord.put(RECORD_KEY, RECORD_VALUE);
    }

    return returnRecord;
  }

  private Utf8 deserializePrimitiveData() throws Exception {
    Utf8 returnData = null;

    // generate schema
    Schema stringSchema = Schema.create(Schema.Type.STRING);

    // setup DatumReader and FileReader
    DatumReader<Utf8> datumReader = new GenericDatumReader<>(stringSchema);
    DataFileReader<Utf8> dataFileReader = new DataFileReader<>(this.serializationFile, datumReader);

    // deserialize
    if (dataFileReader.hasNext()) {
      // single value for construction
      returnData = dataFileReader.next();
    }

    return returnData;
  }

  @Test
  public void remoteSerializationIT() throws Exception {
    boolean actualException = false;

    try {
      // parse protocol
      this.protocol = Protocol.parse(this.protoFile);

      // generate responder
      Responder responder = new GenericResponder(protocol) {
        @Override
        public Object respond(Protocol.Message message, Object request) throws Exception {

          GenericRecord recordRequest = (GenericRecord) request;
          String messageName = message.getName();

          // serialization case
          if (messageName.equals(SERIALIZE_ARRAY_MSG)) {
            // serialize array
            List<Utf8> listValue = (List<Utf8>) recordRequest.get(0);
            serializeArrayData(listValue);
            return new Utf8(ARRAY_SERIALIZATION_ACK);
          }
          if (messageName.equals(SERIALIZE_MAP_MSG)) {
            // serialize map
            Map<Utf8, Utf8> mapValue = (Map<Utf8, Utf8>) recordRequest.get(0);
            serializeMapData(mapValue);
            return new Utf8(MAP_SERIALIZATION_ACK);
          }
          if (messageName.equals(SERIALIZE_RECORD_MSG)) {
            // serialize record
            GenericRecord recordValue = (GenericRecord) recordRequest.get(0);
            serializeRecordData(recordValue);
            return new Utf8(RECORD_SERIALIZATION_ACK);
          }
          if (messageName.equals(SERIALIZE_PRIMITIVE_MSG)) {
            // serialize primitive
            Utf8 stringValue = (Utf8) recordRequest.get(0);
            serializePrimitiveData(stringValue);
            return new Utf8(PRIMITIVE_SERIALIZATION_ACK);
          }

          // deserialization case
          if (messageName.equals(DESERIALIZE_ARRAY_MSG)) {
            // return deserialized array data
            return deserializeArrayData();
          }
          if (messageName.equals(DESERIALIZE_MAP_MSG)) {
            // return deserialized map data
            return deserializeMapData();
          }
          if (messageName.equals(DESERIALIZE_RECORD_MSG)) {
            // return deserialized record data
            return deserializeRecordData();
          }
          if (messageName.equals(DESERIALIZE_PRIMITIVE_MSG)) {
            // return deserialized primitive data
            return deserializePrimitiveData();
          }

          return null;
        }
      };

      // generate server
      switch (this.communicationStatus) {
        case HTTP_COMMUNICATION:
          this.server = new HttpServer(responder, new InetSocketAddress(PORT));
          break;
        case NETTY_COMMUNICATION:
          this.server = new NettyServer(responder, new InetSocketAddress(PORT));
          break;
        case SOCKET_COMMUNICATION:
        default:
          this.server = new SocketServer(responder, new InetSocketAddress(PORT));
          break;
      }

      // start server
      this.server.start();

      // generate clients
      switch (this.communicationStatus) {
        case HTTP_COMMUNICATION:
          this.client1 = new HttpTransceiver(new URL("http://" + HOST + ":" + PORT));
          if (isMultiClient)
            this.client2 = new HttpTransceiver(new URL("http://" + HOST + ":" + PORT));
          else
            this.client2 = null;
          break;
        case NETTY_COMMUNICATION:
          this.client1 = new NettyTransceiver(new InetSocketAddress(HOST, PORT));
          if (this.isMultiClient)
            this.client2 = new NettyTransceiver(new InetSocketAddress(HOST, PORT));
          else {
            this.client2 = null;
          }
          break;
        case SOCKET_COMMUNICATION:
        default:
          this.client1 = new SocketTransceiver(new InetSocketAddress(HOST, PORT));
          if (this.isMultiClient)
            this.client2 = new SocketTransceiver(new InetSocketAddress(HOST, PORT));
          else
            this.client2 = null;
          break;
      }

      // generate serialization requestor
      Requestor client1Requestor = new GenericRequestor(protocol, this.client1);

      // serialization request
      Protocol.Message serializationMessage = protocol.getMessages().get(this.serializeMsg);
      GenericRecord serializationRequest = new GenericData.Record(serializationMessage.getRequest());

      // generate request data
      Utf8 expectedSerializationRequestAck;
      switch (this.dataTypeStatus) {
        case RECORD_DATA:
          Schema schema = protocol.getType(RECORD_TYPE_NAME);
          GenericRecord recordData = new GenericData.Record(schema);
          recordData.put(RECORD_KEY, new Utf8(RECORD_VALUE));
          serializationRequest.put(REQUEST_KEY, recordData);
          expectedSerializationRequestAck = new Utf8(RECORD_SERIALIZATION_ACK);
          break;
        case MAP_DATA:
          Map<String, Utf8> mapData = new HashMap<>();
          mapData.put(MAP_KEY, new Utf8(MAP_VALUE));
          serializationRequest.put(REQUEST_KEY, mapData);
          expectedSerializationRequestAck = new Utf8(MAP_SERIALIZATION_ACK);
          break;
        case ARRAY_DATA:
          List<Utf8> arrayData = new ArrayList<>();
          arrayData.add(new Utf8(ARRAY_VALUE));
          serializationRequest.put(REQUEST_KEY, arrayData);
          expectedSerializationRequestAck = new Utf8(ARRAY_SERIALIZATION_ACK);
          break;
        case PRIMITIVE_DATA:
        default:
          Utf8 primitiveData = new Utf8(PRIMITIVE_VALUE);
          serializationRequest.put(REQUEST_KEY, primitiveData);
          expectedSerializationRequestAck = new Utf8(PRIMITIVE_SERIALIZATION_ACK);
          break;
      }

      // request
      Utf8 actualSerializationResult = (Utf8) client1Requestor.request(this.serializeMsg, serializationRequest);

      // assert on serialization ack
      Assert.assertEquals(expectedSerializationRequestAck, actualSerializationResult);

      // deserialization request
      Protocol.Message deserializationMessage = protocol.getMessages().get(this.deserializeMsg);
      GenericRecord deserializationRequest = new GenericData.Record(deserializationMessage.getRequest());
      Utf8 requestValue = new Utf8(DESERIALIZATION_REQ);
      deserializationRequest.put(REQUEST_KEY, requestValue);

      Object deserializationResult;
      if (this.isMultiClient) {
        // generate requestor for client2
        Requestor client2Requestor = new GenericRequestor(protocol, this.client2);

        // request
        deserializationResult = client2Requestor.request(this.deserializeMsg, deserializationRequest);
      } else {
        // request
        deserializationResult = client1Requestor.request(this.deserializeMsg, deserializationRequest);
      }

      // parse result for data type
      switch (this.dataTypeStatus) {
        case RECORD_DATA:
          // get the record data
          GenericRecord recordValue = (GenericRecord) deserializationResult;

          // assert on field present
          boolean actualHasField = recordValue.hasField(RECORD_KEY);
          Assert.assertTrue(actualHasField);

          // assert on field value
          Utf8 actualFieldValue = (Utf8) recordValue.get(RECORD_KEY);
          Utf8 expectedFieldValue = new Utf8(RECORD_VALUE);
          Assert.assertEquals(expectedFieldValue, actualFieldValue);
          break;
        case MAP_DATA:
          // get map data
          Map<Utf8, Utf8> mapValue = (Map<Utf8, Utf8>) deserializationResult;

          // assert on size
          int expectedMapSize = 1;
          int actualMapSize = mapValue.size();
          Assert.assertEquals(expectedMapSize, actualMapSize);

          // assert on keys
          boolean actualHasKey = mapValue.containsKey(new Utf8(MAP_KEY));
          Assert.assertTrue(actualHasKey);

          // assert on value
          Utf8 expectedMapValue = new Utf8(MAP_VALUE);
          Utf8 actualMapValue = mapValue.get(new Utf8(MAP_KEY));
          Assert.assertEquals(expectedMapValue, actualMapValue);
          break;
        case ARRAY_DATA:
          // get the array data
          List<Utf8> listValue = (List<Utf8>) deserializationResult;

          // assert on array dimension
          int expectedArraySize = 1;
          int actualArraySize = listValue.size();
          Assert.assertEquals(expectedArraySize, actualArraySize);

          // assert on string value
          Utf8 expectedString = new Utf8(ARRAY_VALUE);
          Utf8 actualString = listValue.get(0);
          Assert.assertEquals(expectedString, actualString);
          break;
        case PRIMITIVE_DATA:
        default:
          // assert on value
          Utf8 actualValue = (Utf8) deserializationResult;
          Utf8 expectedValue = new Utf8(PRIMITIVE_VALUE);
          Assert.assertEquals(expectedValue, actualValue);
          break;
      }

    } catch (Exception e) {
      actualException = true;
    }

    // assert on Exception thrown
    Assert.assertEquals(this.expectedException, actualException);
  }


}
