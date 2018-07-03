package org.verdictdb.jdbc41;

import java.io.File;
import java.sql.DriverManager;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.HiveSyntax;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;

@RunWith(StandaloneHiveRunner.class)
public class JdbcResultSetMetaDataHiveTest {
  
  @HiveSQL(files = {})

  private HiveShell shell;
  
  private DbmsConnection conn;

  @Before
  public void setupSourceDatabase() throws Exception {
      shell.execute("CREATE DATABASE tpchmetadatatest");
      shell.execute("USE tpchmetadatatest");
      File schemaFile = new File("src/test/resources/tpch-schema.sql");
      String schemas = Files.toString(schemaFile, Charsets.UTF_8);
      for (String schema : schemas.split(";")) {
          schema += ";"; // add semicolon at the end
          schema = schema.trim();
          shell.execute(schema);
      }
      
      String jdbcUrl = shell.getHiveConf().get("javax.jdo.option.ConnectionURL");
//      System.out.println(jdbcUrl);
      // This does not work
//      conn = new JdbcConnection(DriverManager.getConnection(jdbcUrl), new HiveSyntax());
  }
  
//  @Test
//  public void testLineitem() throws VerdictDBException {
//    System.out.println(conn.getSchemas());
//  }


}
