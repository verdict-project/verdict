package org.verdictdb.core.connection;

import java.sql.Connection;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
//import org.apache.spark.SparkContext;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.SqlSyntax;

public class SparkConnection implements DbmsConnection {
  
  
  public SparkConnection() {
    
  }
  
//  public SparkConnection(SparkContext sc) {
//    
//  }

  @Override
  public List<String> getSchemas() throws VerdictDBDbmsException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> getTables(String schema) throws VerdictDBDbmsException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Pair<String, Integer>> getColumns(String schema, String table) throws VerdictDBDbmsException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> getPartitionColumns(String schema, String table) throws VerdictDBDbmsException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getDefaultSchema() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DbmsQueryResult execute(String query) throws VerdictDBDbmsException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SqlSyntax getSyntax() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Connection getConnection() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

}
