package org.apache.avro.ipc.generic;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.ipc.CallFuture;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.SocketServer;
import org.apache.avro.ipc.SocketTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.utils.Utils;
import org.apache.avro.util.Utf8;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.mockito.Mockito;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.avro.ipc.utils.Utils.MessageStatus.*;
import static org.apache.avro.ipc.utils.Utils.ProtocolStatus.*;
import static org.apache.avro.ipc.utils.Utils.ServerStatus.SERVER_NOT_STARTED;
import static org.apache.avro.ipc.utils.Utils.ServerStatus.SERVER_UP;

@RunWith(value = Parameterized.class)
public class GenericRequestorTest {

  private final String VALID_PROTOCOL_PATH = "./src/test/resources/protocol.avpr";
  private final String NOT_VALID_PROTOCOL_PATH = "./src/test/resources/notValid.avpr";
  private final String NOT_EXISTING_PROTOCOL_PATH = "./src/test/resources/notExisting.avpr";
  private final String PRIMITIVE_MSG_NAME = "send_primitive";
  private final String ONE_WAY_MSG_NAME = "send_one_way";
  private final String ERROR_MSG_NAME = "send_error";
  private final String CALLBACK_MSG_NAME = "send_callback";
  private final String COMPLEX_MSG_NAME = "send_complex";
  private final String NOT_EXISTING_MSG_NAME = "notExist";
  private final String ERROR_MSG_VALUE = "error_test";
  private final String SERVER_ACK = "ACK SERVER";
  private final String CLIENT_REQ_VALUE = "Client request";
  private final String COMPLEX_TYPE_NAME = "Custom";
  private final String FIELD_1_NAME = "to";
  private final String FIELD_2_NAME = "from";
  private final String FIELD_1_VALUE = "server_id";
  private final String FIELD_2_VALUE = "client_id";
  private final String HOST = "localhost";
  private final int PORT = 1234;
  private final String REQ_NAME = "req";
  private String msgName;
  private String protoPath;
  private boolean isServerUp;
  private boolean isWithCallback;
  private boolean isComplexRequest;
  private boolean expectedException;
  private boolean expectedOneWay;
  private File protoFile;
  private Server server;
  private Transceiver client;

  @Parameterized.Parameters
  public static Collection<Object> testParameters() {
    return Arrays.asList(new Object[][] {
        // protocolStatus, messageStatus, serverStatus, expectedException

        { VALID_PROTOCOL, PRIMITIVE_MSG, SERVER_UP, false }, { VALID_PROTOCOL, ONE_WAY_MSG, SERVER_UP, false },
        { VALID_PROTOCOL, CALLBACK_MSG, SERVER_UP, false }, { VALID_PROTOCOL, COMPLEX_MSG, SERVER_UP, false },
        { VALID_PROTOCOL, ERROR_MSG, SERVER_UP, true }, { VALID_PROTOCOL, PRIMITIVE_MSG, SERVER_NOT_STARTED, true },
        { VALID_PROTOCOL, NOT_EXISTING_MSG, SERVER_UP, true }, { NOT_VALID_PROTOCOL, PRIMITIVE_MSG, SERVER_UP, true },
        { NOT_VALID_PROTOCOL, NOT_EXISTING_MSG, SERVER_UP, true },
        { NOT_EXISTING_PROTOCOL, PRIMITIVE_MSG, SERVER_UP, true },
        { NOT_EXISTING_PROTOCOL, NOT_EXISTING_MSG, SERVER_UP, true } });
  }

  public GenericRequestorTest(Utils.ProtocolStatus protoStatus, Utils.MessageStatus msgStatus,
      Utils.ServerStatus serverStatus, boolean expectedException) {
    this.expectedException = expectedException;
    this.configureMsgName(msgStatus);
    this.configureProtocolPath(protoStatus);
    this.configureIsServerUp(serverStatus);
  }

  private void configureIsServerUp(Utils.ServerStatus serverStatus) {
    switch (serverStatus) {
    case SERVER_NOT_STARTED:
      this.isServerUp = false;
      break;
    case SERVER_UP:
    default:
      this.isServerUp = true;
      break;
    }
  }

  private void configureProtocolPath(Utils.ProtocolStatus protoStatus) {
    switch (protoStatus) {
    case NOT_VALID_PROTOCOL:
      this.protoPath = NOT_VALID_PROTOCOL_PATH;
      break;
    case NOT_EXISTING_PROTOCOL:
      this.protoPath = NOT_EXISTING_PROTOCOL_PATH;
      break;
    case VALID_PROTOCOL:
    default:
      this.protoPath = VALID_PROTOCOL_PATH;
      break;

    }
  }

  private void configureMsgName(Utils.MessageStatus msgStatus) {
    switch (msgStatus) {
    case NOT_EXISTING_MSG:
      this.msgName = NOT_EXISTING_MSG_NAME;
      this.expectedOneWay = false;
      this.isWithCallback = false;
      this.isComplexRequest = false;
      break;
    case ERROR_MSG:
      this.msgName = ERROR_MSG_NAME;
      this.expectedOneWay = false;
      this.isWithCallback = false;
      this.isComplexRequest = false;
      break;
    case ONE_WAY_MSG:
      this.msgName = ONE_WAY_MSG_NAME;
      this.expectedOneWay = true;
      this.isWithCallback = false;
      this.isComplexRequest = false;
      break;
    case CALLBACK_MSG:
      this.msgName = CALLBACK_MSG_NAME;
      this.expectedOneWay = false;
      this.isWithCallback = true;
      this.isComplexRequest = false;
      break;
    case COMPLEX_MSG:
      this.msgName = COMPLEX_MSG_NAME;
      this.expectedOneWay = false;
      this.isWithCallback = false;
      this.isComplexRequest = true;
      break;
    case PRIMITIVE_MSG:
    default:
      this.msgName = PRIMITIVE_MSG_NAME;
      this.expectedOneWay = false;
      this.isWithCallback = false;
      this.isComplexRequest = false;
      break;
    }
  }

  @Before
  public void setupProtoFile() {
    this.protoFile = new File(this.protoPath);
  }

  @Test
  public void communicationTest() throws Exception {
    boolean actualException = false;

    try {
      // parse protocol
      Protocol proto = Protocol.parse(this.protoFile);

      // generate responder
      Responder responder = new GenericResponder(proto) {

        @Override
        public Object respond(Protocol.Message message, Object request) throws Exception {
          GenericRecord record = (GenericRecord) request;
          Utf8 actualValue;
          Utf8 expectedValue;

          // assert expected message and actual
          if (message.getName().equals(COMPLEX_MSG_NAME)) {
            // complex case
            GenericRecord recordRead = (GenericRecord) record.get(0);
            actualValue = (Utf8) recordRead.get(FIELD_1_NAME);
            expectedValue = new Utf8(FIELD_1_VALUE);

            // Assert first value
            Assert.assertEquals(expectedValue, actualValue);

            actualValue = (Utf8) recordRead.get(FIELD_2_NAME);
            expectedValue = new Utf8(FIELD_2_VALUE);

            // Assert second value
            Assert.assertEquals(expectedValue, actualValue);

          } else {
            // not complex case
            actualValue = (Utf8) record.get(0);
            expectedValue = new Utf8(CLIENT_REQ_VALUE);

            Assert.assertEquals(expectedValue, actualValue);
          }

          // generate response
          if (message.getName().equals(ONE_WAY_MSG_NAME) || message.getName().equals(CALLBACK_MSG_NAME))
            return null;
          if (message.getName().equals(ERROR_MSG_NAME)) {
            throw new AvroRemoteException(ERROR_MSG_VALUE);
          }

          // primitive and complex cases
          return new Utf8(SERVER_ACK);
        }
      };

      // generate server and start
      if (this.isServerUp) {
        this.server = new SocketServer(responder, new InetSocketAddress(PORT));
        this.server.start();
      } else
        this.server = null;

      // generate client and requestor
      this.client = new SocketTransceiver(new InetSocketAddress(HOST, PORT));
      GenericRequestor requestor = new GenericRequestor(proto, this.client);

      // assert on generic data generated with the constructor
      GenericData actualData = requestor.getGenericData();
      GenericData expectedData = GenericData.get();
      Assert.assertEquals(expectedData, actualData);

      // message section
      Protocol.Message message = proto.getMessages().get(this.msgName);
      boolean actualIsOneWay = message.isOneWay();

      // assert messsage is one-way
      Assert.assertEquals(expectedOneWay, actualIsOneWay);

      GenericRecord request = new GenericData.Record(message.getRequest());

      // generate requestData
      if (this.isComplexRequest) {
        // build the Custom type declared in the protocol file
        Schema requestSchema = proto.getType(COMPLEX_TYPE_NAME);
        GenericRecord requestValue = new GenericData.Record(requestSchema);
        requestValue.put(FIELD_1_NAME, new Utf8(FIELD_1_VALUE));
        requestValue.put(FIELD_2_NAME, new Utf8(FIELD_2_VALUE));
        request.put(REQ_NAME, requestValue);

      } else {
        Utf8 requestValue = new Utf8(CLIENT_REQ_VALUE);
        request.put(REQ_NAME, requestValue);
      }

      // check if callback case
      if (this.isWithCallback) {
        // Mock the callback
        CallFuture<Utf8> mockedCallFuture = (CallFuture<Utf8>) Mockito.mock(CallFuture.class);
        requestor.request(this.msgName, request, mockedCallFuture);

        // verify if the handle result is invoked
        Mockito.verify(mockedCallFuture, Mockito.times(1)).handleResult(Mockito.any());
      } else {
        Utf8 actualValue = (Utf8) requestor.request(this.msgName, request);
        Utf8 expectedValue;

        if (this.msgName.equals(ONE_WAY_MSG_NAME))
          expectedValue = null;
        else
          expectedValue = new Utf8(SERVER_ACK);

        // Assert on response
        Assert.assertEquals(expectedValue, actualValue);
      }

    } catch (Exception e) {
      actualException = true;

      if (this.msgName.equals(ERROR_MSG_NAME)) {
        String actualExceptionMessage = e.getMessage();
        Assert.assertEquals(ERROR_MSG_VALUE, actualExceptionMessage);
      }
    }

    // assert on Exception thrown
    Assert.assertEquals(this.expectedException, actualException);

  }

  @After
  public void closeAll() {
    try {
      if (!this.expectedException || this.msgName.equals(ERROR_MSG_NAME)) {
        this.server.close();
        this.client.close();
      }
    } catch (Exception e) {
      Assert.fail();
    }
  }

}
