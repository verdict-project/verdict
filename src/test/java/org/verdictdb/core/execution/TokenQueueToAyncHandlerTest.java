package org.verdictdb.core.execution;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.sql.NonValidatingSQLParser;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.resulthandler.TokenQueueToAyncHandler;
import org.verdictdb.resulthandler.StandardOutputHandler;
import org.verdictdb.sql.syntax.H2Syntax;

import java.sql.DriverManager;
import java.sql.SQLException;

public class TokenQueueToAyncHandlerTest {

  static String originalSchema = "originalschema";

  static String originalTable = "originaltable";

  static String newSchema = "newschema";

  static String newTable  = "newtable";

  static int aggblockCount = 2;

  static DbmsConnection conn;

  @BeforeClass
  public static void setupDbConnAndScrambledTable() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:createasselecttest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = new JdbcConnection(DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD), new H2Syntax());
    conn.executeUpdate(String.format("CREATE SCHEMA IF NOT EXISTS\"%s\"", originalSchema));
    conn.executeUpdate(String.format("CREATE SCHEMA IF NOT EXISTS\"%s\"", newSchema));
    populateData(conn, originalSchema, originalTable);
  }

  static void populateData(DbmsConnection conn, String schemaName, String tableName) throws VerdictDBDbmsException {
    conn.executeUpdate(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", schemaName, tableName));
    for (int i = 0; i < 10; i++) {
      conn.executeUpdate(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          schemaName, tableName, i, (double) i+1));
    }
  }

  @Test
  public void simpleAggregateTest() throws VerdictDBException {
    String sql = "select avg(t.value) as a from originalschema.originaltable as t;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan(newSchema, null, selectQuery);
    TokenQueueToAyncHandler tokenQueueToAyncHandler = new TokenQueueToAyncHandler(queryExecutionPlan, new ExecutionTokenQueue());
    tokenQueueToAyncHandler.setHandler(new StandardOutputHandler());
    queryExecutionPlan.root.executeAndWaitForTermination(conn);
    tokenQueueToAyncHandler.execute();
  }

  @Test
  public void nestedAggregateFromTest() throws VerdictDBException {
    String sql = "select avg(t.value) from (select o.value from originalschema.originaltable as o where o.value>5) as t;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan(newSchema, null, selectQuery);
    TokenQueueToAyncHandler tokenQueueToAyncHandler = new TokenQueueToAyncHandler(queryExecutionPlan, new ExecutionTokenQueue());
    tokenQueueToAyncHandler.setHandler(new StandardOutputHandler());
    queryExecutionPlan.root.executeAndWaitForTermination(conn);
    tokenQueueToAyncHandler.execute();
  }

  @Test
  public void nestedAggregateFilterTest() throws VerdictDBException {
    String sql = "select avg(t.value) as a from originalschema.originaltable as t where t.value > " +
        "(select avg(o.value) as avg_value from originalschema.originaltable as o);";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan(newSchema, null, selectQuery);
    TokenQueueToAyncHandler tokenQueueToAyncHandler = new TokenQueueToAyncHandler(queryExecutionPlan, new ExecutionTokenQueue());
    tokenQueueToAyncHandler.setHandler(new StandardOutputHandler());
    queryExecutionPlan.root.executeAndWaitForTermination(conn);
    tokenQueueToAyncHandler.execute();
  }

  @Test
  public void aggregateWithGroupbyTest() throws VerdictDBException {
    String sql = "select t.id as id, avg(t.value) as a from originalschema.originaltable as t group by id;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan(newSchema, null, selectQuery);
    TokenQueueToAyncHandler tokenQueueToAyncHandler = new TokenQueueToAyncHandler(queryExecutionPlan, new ExecutionTokenQueue());
    tokenQueueToAyncHandler.setHandler(new StandardOutputHandler());
    queryExecutionPlan.root.executeAndWaitForTermination(conn);
    tokenQueueToAyncHandler.execute();
  }

  @Test
  public void nestedAggregateWithGroupbyTest() throws VerdictDBException {
    String sql = "select t.id as id, avg(t.value) as a from (select o.id, o.value from originalschema.originaltable as o where o.value>5) as t group by id;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan(newSchema, null, selectQuery);
    TokenQueueToAyncHandler tokenQueueToAyncHandler = new TokenQueueToAyncHandler(queryExecutionPlan, new ExecutionTokenQueue());
    tokenQueueToAyncHandler.setHandler(new StandardOutputHandler());
    queryExecutionPlan.root.executeAndWaitForTermination(conn);
    tokenQueueToAyncHandler.execute();
  }

}
