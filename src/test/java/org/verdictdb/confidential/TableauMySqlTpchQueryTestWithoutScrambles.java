package org.verdictdb.confidential;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.VerdictContext;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.coordinator.VerdictSingleResult;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class TableauMySqlTpchQueryTestWithoutScrambles {

  static Connection conn;

  static DbmsConnection dbmsConnection;

  static Statement stmt;

  private static final String MYSQL_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE = "tpch_flat_orc_2";

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn = DatabaseConnectionHelpers.setupMySql(
        mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    dbmsConnection = JdbcConnection.create(conn);
    dbmsConnection.setDefaultSchema(MYSQL_DATABASE);
    stmt = conn.createStatement();
    stmt.execute("create schema if not exists `verdictdb_temp`");
  }

  public Pair<VerdictSingleResult, ResultSet> getAnswer(int queryNum) throws IOException, VerdictDBException, SQLException {
    String filename = "tpchMySqlQuery" + queryNum + ".sql";
    String path = "src/test/resources/confidential/" + filename;
    File queryFile = new File(path);
    String sql = Files.toString(queryFile, Charsets.UTF_8);

    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    return new ImmutablePair<>(result, rs);
  }

  @Test
  public void Query1Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(1);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getDouble(0), rs.getDouble(1), 1e-5);
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
      assertEquals(result.getDouble(2), rs.getDouble(3), 1e-5);
      assertEquals(result.getString(3), rs.getString(4));
      assertEquals(result.getString(4), rs.getString(5));
      assertEquals(result.getDouble(5), rs.getDouble(6), 1e-5);
      assertEquals(result.getDouble(6), rs.getDouble(7), 1e-5);
      assertEquals(result.getDouble(7), rs.getDouble(8), 1e-5);
      assertEquals(result.getDouble(8), rs.getDouble(9), 1e-5);
      assertEquals(result.getDouble(9), rs.getDouble(10), 1e-5);
    }
  }

  @Test
  public void Query2Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(2);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getInt(2), rs.getInt(3));
      assertEquals(result.getInt(3), rs.getInt(4));
      assertEquals(result.getString(4), rs.getString(5));
      assertEquals(result.getString(5), rs.getString(6));
      assertEquals(result.getString(6), rs.getString(7));
      assertEquals(result.getString(7), rs.getString(8));
      assertEquals(result.getDouble(8), rs.getDouble(9), 1e-5);
    }
  }

  @Test
  public void Query3Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(3);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getDate(0), rs.getDate(1));
      assertEquals(result.getInt(1), rs.getInt(2));
      assertEquals(result.getInt(2), rs.getInt(3));
      assertEquals(result.getDouble(3), rs.getDouble(4), 1e-5);
    }
  }

  @Test
  public void Query4Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(4);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getLong(0), rs.getLong(1));
      assertEquals(result.getString(1), rs.getString(2));
    }
  }

  @Test
  public void Query5Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(5);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
    }
  }

  @Test
  public void Query6Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(6);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getDouble(0), rs.getDouble(1), 1e-5);
    }
  }

  @Test
  public void Query7Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(7);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getDouble(2), rs.getDouble(3), 1e-5);
      assertEquals(result.getDouble(3), rs.getDouble(4), 1e-5);
    }
  }

  @Test
  public void Query8Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(8);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
      assertEquals(result.getInt(2), rs.getInt(3));
    }
  }

  @Test
  public void Query9Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(9);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
      assertEquals(result.getInt(2), rs.getInt(3));
    }
  }

  @Test
  public void Query10Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(10);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getDouble(0), rs.getDouble(1), 1e-5);
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getString(2), rs.getString(3));
      assertEquals(result.getInt(3), rs.getInt(4));
      assertEquals(result.getString(4), rs.getString(5));
      assertEquals(result.getString(5), rs.getString(6));
      assertEquals(result.getString(6), rs.getString(7));
      assertEquals(result.getDouble(7), rs.getDouble(8), 1e-5);
    }
  }

  @Test
  public void Query11Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(11);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getInt(0), rs.getInt(1));
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
    }
  }

  @Test
  public void Query12Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(12);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getDouble(2), rs.getDouble(3), 1e-5);
    }
  }

  @Test
  public void Query13Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(13);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getInt(0), rs.getInt(1));
      assertEquals(result.getInt(1), rs.getInt(2));
    }
  }

  @Test
  public void Query14Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(14);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getInt(0), rs.getInt(1));
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
    }
  }

  @Test
  public void Query15Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(15);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getInt(0), rs.getInt(1));
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getString(2), rs.getString(3));
      assertEquals(result.getString(3), rs.getString(4));
      assertEquals(result.getDouble(4), rs.getDouble(5), 1e-5);
    }
  }

  @Test
  public void Query16Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(16);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getInt(0), rs.getInt(1));
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getInt(2), rs.getInt(3));
      assertEquals(result.getString(3), rs.getString(4));
    }
  }

  @Test
  public void Query17Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(17);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getDouble(0), rs.getDouble(1), 1e-5);
    }
  }

  @Test
  public void Query18Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(18);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getInt(0), rs.getInt(1));
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getDouble(2), rs.getDouble(3),1e-5);
      assertEquals(result.getString(3), rs.getString(4));
      assertEquals(result.getInt(4), rs.getInt(5));
      assertEquals(result.getDouble(5), rs.getDouble(6),1e-5);
      assertEquals(result.getDouble(6), rs.getDouble(7),1e-5);
    }
  }

  @Test
  public void Query19Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(19);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getDouble(0), rs.getDouble(1),1e-5);
    }
  }

  @Test
  public void Query20Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(20);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getString(1), rs.getString(2));
    }
  }

  @Test
  public void Query21Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(21);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
      assertEquals(result.getDouble(2), rs.getDouble(3), 1e-5);
    }
  }

  @Test
  public void Query22Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(22);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
      assertEquals(result.getDouble(2), rs.getDouble(3), 1e-5);
    }
  }


}
