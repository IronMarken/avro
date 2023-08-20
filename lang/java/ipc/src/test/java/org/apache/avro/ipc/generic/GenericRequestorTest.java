package org.apache.avro.ipc.generic;

import org.apache.avro.Protocol;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.ipc.Requestor;
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

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.avro.ipc.utils.Utils.MessageStatus.EXISTING_MSG;
import static org.apache.avro.ipc.utils.Utils.MessageStatus.NOT_EXISTING_MSG;
import static org.apache.avro.ipc.utils.Utils.ProtocolStatus.*;

@RunWith(value = Parameterized.class)
public class GenericRequestorTest {

  private final String VALID_PROTOCOL_PATH = "./src/test/resources/protocol.avpr";
  private final String NOT_VALID_PROTOCOL_PATH = "./src/test/resources/notValid.avpr";
  private final String NOT_EXISTING_PROTOCOL_PATH = "./src/test/resources/notExisting.avpr";
  private final String VALID_MSG_NAME = "send";
  private final String NOT_EXISTING_MSG_NAME = "notExist";
  private final String SERVER_ACK = "ACK SERVER";
  private final String CLIENT_REQ_VALUE = "Client request";
  private final String HOST = "localhost";
  private final int PORT = 1234;
  private final String REQ_NAME = "req";
  private String msgName;
  private String protoPath;
  private boolean expectedException;

  private File protoFile;
  private Server server;
  private Transceiver client;

  @Parameterized.Parameters
  public static Collection<Object> testParameters() {
    return Arrays.asList(new Object[][] {
        // protocolStatus, messageStatus, expectedException
        { VALID_PROTOCOL, EXISTING_MSG, false }, { VALID_PROTOCOL, NOT_EXISTING_MSG, true },
        { NOT_VALID_PROTOCOL, EXISTING_MSG, true }, { NOT_VALID_PROTOCOL, NOT_EXISTING_MSG, true },
        { NOT_EXISTING_PROTOCOL, EXISTING_MSG, true }, { NOT_EXISTING_PROTOCOL, NOT_EXISTING_MSG, true } });
  }

  public GenericRequestorTest(Utils.ProtocolStatus protoStatus, Utils.MessageStatus msgStatus,
      boolean expectedException) {
    this.expectedException = expectedException;
    this.configureMsgName(msgStatus);
    this.configureProtocolPath(protoStatus);
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
      break;
    case EXISTING_MSG:
    default:
      this.msgName = VALID_MSG_NAME;
      break;
    }
  }

  @Before
  public void setupProtoFile() {
    this.protoFile = new File(this.protoPath);
  }

  @Test
  public void communicationTest() {
    boolean actualException = false;

    try {
      // parse protocol
      Protocol proto = Protocol.parse(this.protoFile);

      // generate responder
      Responder responder = new GenericResponder(proto) {
        @Override
        public Object respond(Protocol.Message message, Object request) throws Exception {
          GenericRecord record = (GenericRecord) request;
          Utf8 actualValue = (Utf8) record.get(0);
          Utf8 expectedValue = new Utf8(CLIENT_REQ_VALUE);

          // assert expected message and actual
          Assert.assertEquals(expectedValue, actualValue);

          // generate response
          return new Utf8(SERVER_ACK);
        }
      };

      // generate server and start
      this.server = new SocketServer(responder, new InetSocketAddress(PORT));
      server.start();

      // generate client and requestor
      this.client = new SocketTransceiver(new InetSocketAddress(HOST, PORT));
      Requestor requestor = new GenericRequestor(proto, this.client);

      Utf8 requestValue = new Utf8(CLIENT_REQ_VALUE);

      GenericRecord request = new GenericData.Record(proto.getMessages().get(this.msgName).getRequest());

      request.put(REQ_NAME, requestValue);

      Utf8 actualValue = (Utf8) requestor.request(this.msgName, request);

      Utf8 expectedValue = new Utf8(SERVER_ACK);

      // Assert on response
      Assert.assertEquals(expectedValue, actualValue);

    } catch (Exception e) {
      actualException = true;
    }

    // assert on Exception thrown
    Assert.assertEquals(this.expectedException, actualException);
  }

  @After
  public void closeAll() {
    try {
      if (!this.expectedException) {
        this.server.close();
        this.client.close();
      }
    } catch (Exception e) {
      Assert.fail();
    }
  }

}
