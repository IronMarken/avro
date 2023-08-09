package org.apache.avro.generic;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MockitoGenericDatumReaderTest {

  Schema mockedSchema;
  GenericDatumReader genericDatumReader;

  @Before
  public void setup() {
    mockedSchema = Mockito.mock(Schema.class);
    genericDatumReader = new GenericDatumReader(mockedSchema, mockedSchema);

  }

  @Ignore
  @Test
  public void dummyMockitoTest() {
    Schema schema = genericDatumReader.getSchema();
    Assert.assertEquals(mockedSchema, schema);
  }

}
