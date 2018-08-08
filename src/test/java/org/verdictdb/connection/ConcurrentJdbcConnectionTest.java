package org.verdictdb.connection;

import static org.junit.Assert.*;

import org.junit.Test;
import org.verdictdb.exception.VerdictDBDbmsException;

public class ConcurrentJdbcConnectionTest {

  @Test
  public void test() throws VerdictDBDbmsException {
    String url = "jdbc:mysql://localhost?user=root";
    DbmsConnection c = ConcurrentJdbcConnection.create(url);
    c.getSchemas();
    c.close();
  }

}
