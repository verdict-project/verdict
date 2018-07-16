package org.verdictdb.connection;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.exception.VerdictDBDbmsException;

public interface MetaDataProvider {
  
  public List<String> getSchemas() throws VerdictDBDbmsException;

  List<String> getTables(String schema) throws VerdictDBDbmsException;

  public List<Pair<String, String>> getColumns(String schema, String table) throws VerdictDBDbmsException;

  public List<String> getPartitionColumns(String schema, String table) throws VerdictDBDbmsException;

  public String getDefaultSchema();
  
  public void setDefaultSchema(String schema);
  
}
