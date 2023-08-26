package org.apache.avro.utils;

public class Utils {

  public enum CommunicationStatus {
    SOCKET_COMMUNICATION, NETTY_COMMUNICATION, HTTP_COMMUNICATION
  }

  public enum ClientStatus {
    SINGLE_CLIENT, MULTI_CLIENT
  }

  public enum ProtocolStatus {
    VALID_PROTOCOL, NOT_VALID_PROTOCOL, NOT_EXISTING_PROTOCOL
  }

  public enum DataTypeStatus {
    PRIMITIVE_DATA, RECORD_DATA, MAP_DATA, ARRAY_DATA
  }

}
