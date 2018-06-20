package org.verdictdb.connection;

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

public interface MetaDataProvider {
  
  public List<String> getSchemas() throws SQLException;

  List<String> getTables(String schema) throws SQLException;

  public List<Pair<String, Integer>> getColumns(String schema, String table) throws SQLException;

  public List<String> getPartitionColumns(String schema, String table) throws SQLException;

}
