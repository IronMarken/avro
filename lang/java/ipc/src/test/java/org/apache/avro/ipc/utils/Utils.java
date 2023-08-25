package org.apache.avro.ipc.utils;

public class Utils {

  public enum ProtocolStatus {
    VALID_PROTOCOL, NOT_VALID_PROTOCOL, NOT_EXISTING_PROTOCOL
  }

  public enum MessageStatus {
    PRIMITIVE_MSG, NOT_EXISTING_MSG, COMPLEX_MSG, ERROR_MSG, ONE_WAY_MSG, CALLBACK_MSG
  }

  public enum ServerStatus {
    SERVER_UP, SERVER_NOT_STARTED
  }
}
