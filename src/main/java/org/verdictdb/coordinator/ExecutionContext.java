/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.coordinator;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.VerdictResultStream;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.CreateScrambleQuery;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.metastore.CachedScrambleMetaStore;
import org.verdictdb.metastore.ScrambleMetaStore;
import org.verdictdb.metastore.VerdictMetaStore;
import org.verdictdb.parser.VerdictSQLParser;
import org.verdictdb.parser.VerdictSQLParser.IdContext;
import org.verdictdb.parser.VerdictSQLParserBaseVisitor;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationGen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.verdictdb.coordinator.VerdictSingleResultFromListData.createWithSingleColumn;

/**
 * Stores the context for a single query execution. Includes both scrambling query and select query.
 *
 * @author Yongjoo Park
 */
public class ExecutionContext {

  private DbmsConnection conn;

  private VerdictMetaStore metaStore;

  private QueryContext queryContext;

  private Coordinator runningCoordinator = null;

  private final long serialNumber;

  private final VerdictDBLogger log = VerdictDBLogger.getLogger(getClass());

  private VerdictOption options;

  public enum QueryType {
    select, scrambling, drop_scramble, drop_all_scrambles, set_default_schema, unknown, show_databases, show_tables, show_scrambles, describe_table
  }

  /**
   * @param conn
   *          DbmsConnection
   * @param contextId
   *          parent's context id
   * @param serialNumber
   *          serial number of this ExecutionContext
   * @param options
   */
  public ExecutionContext(DbmsConnection conn, VerdictMetaStore metaStore, String contextId,
      long serialNumber, VerdictOption options) {
    this.conn = conn;
    this.metaStore = metaStore;
    this.serialNumber = serialNumber;
    this.queryContext = new QueryContext(contextId, serialNumber);
    this.options = options;
  }

  public long getExecutionContextSerialNumber() {
    return serialNumber;
  }

  /**
   * Check whether given sql contains 'bypass' keyword at the beginning
   *
   * @param sql
   *          original sql
   * @return without 'bypass' keyword if the original sql begins with it. null otherwise.
   */
  private String checkBypass(String sql) {
    if (sql.trim().toLowerCase().startsWith("bypass")) {
      return sql.trim().substring(6);
    }
    return null;
  }

  private VerdictSingleResult executeAsIs(String sql) throws VerdictDBDbmsException {
    return new VerdictSingleResultFromDbmsQueryResult(conn.execute(sql));
  }

  public VerdictSingleResult sql(String query) throws VerdictDBException {
    return this.sql(query, true);
  }

  public VerdictSingleResult sql(String query, boolean getResult) throws VerdictDBException {
    String bypassSql = checkBypass(query);
    if (bypassSql != null) {
      return executeAsIs(bypassSql);
    } else {
      QueryType queryType = identifyQueryType(query);
      if ((queryType != QueryType.select && queryType != QueryType.show_databases
          && queryType != QueryType.show_tables && queryType != QueryType.describe_table
          && queryType != QueryType.show_scrambles) && getResult) {
        throw new VerdictDBException("Can not issue data manipulation statements with executeQuery().");
      }

      VerdictResultStream stream = streamsql(query);

      if (stream == null) {
        return null;
      }
      QueryResultAccuracyEstimator accEst = new QueryResultAccuracyEstimatorFromDifference(
          runningCoordinator);
      try {
        while (stream.hasNext()) {
          VerdictSingleResult rs = stream.next();
          accEst.add(rs);
          if (accEst.isLastResultAccurate()) {
            return rs;
          }
        }
        // return the last result otherwise
        return accEst.getAnswers().get(accEst.getAnswerCount() - 1);
      } catch (RuntimeException e) {
        throw e;
      } finally {
        stream.close();
        abort();
      }
    }
  }

  // not used for now due to instability (i.e., often fails to achieve the goal).
  private void abortInParallel(VerdictResultStream stream) {
    Thread task = new Thread(new ConcurrentAborter(stream));
    task.start();
  }

  class ConcurrentAborter implements Runnable {

    VerdictResultStream stream;

    public ConcurrentAborter(VerdictResultStream stream) {
      this.stream = stream;
    }

    @Override
    public void run() {
      stream.close();
      abort();
    }
  }

  public VerdictResultStream streamsql(String query) throws VerdictDBException {
    // determines the type of the given query and forward it to an appropriate coordinator.
    QueryType queryType = identifyQueryType(query);

    if (queryType.equals(QueryType.select)) {
      log.debug("Query type: select");
      ScrambleMetaSet metaset = metaStore.retrieve();
      SelectQueryCoordinator coordinator = new SelectQueryCoordinator(conn, metaset, options);
      runningCoordinator = null;

      ExecutionResultReader reader = coordinator.process(query, queryContext);
      if (coordinator.getLastQuery() != null) {
        // this means there are scrambles for the query so that
        // we need to abort the coordinator at the end.
        runningCoordinator = coordinator;
      }
      VerdictResultStream stream = new VerdictResultStreamFromExecutionResultReader(reader, this);
      return stream;

    } else if (queryType.equals(QueryType.scrambling)) {
      log.debug("Query type: scrambling");
      CreateScrambleQuery scrambleQuery = generateScrambleQuery(query);
      scrambleQuery.checkIfSupported(); // checks the validity; throws an exception if not.

      ScramblingCoordinator scrambler = new ScramblingCoordinator(conn, scrambleQuery.getNewSchema(),
          options.getVerdictTempSchemaName(), scrambleQuery.getBlockSize());

      // store this metadata to our own metadata db.
      ScrambleMeta meta = scrambler.scramble(scrambleQuery);

      // Add metadata to metastore
      ScrambleMetaStore metaStore = new ScrambleMetaStore(conn, options);
      metaStore.addToStore(meta);
      refreshScrambleMetaStore();
      return null;

    } else if (queryType.equals(QueryType.drop_scramble)) {
      log.debug("Query type: drop_scramble");

      ScrambleMetaStore metaStore = new ScrambleMetaStore(conn, options);
      Pair<BaseTable, BaseTable> tablePair = getTablePairForDropScramble(query);
      metaStore.dropScrambleTable(tablePair.getLeft(), tablePair.getRight());
      return null;

    } else if (queryType.equals(QueryType.drop_all_scrambles)) {
      log.debug("Query type: drop_all_scrambles");

      ScrambleMetaStore metaStore = new ScrambleMetaStore(conn, options);
      BaseTable table = getTableForDropAllScramble(query);
      metaStore.dropAllScrambleTable(table);
      return null;

    } else if (queryType.equals(QueryType.show_scrambles)) {
      log.debug("Query type: show_scrambles");

      ScrambleMetaStore metaStore = new ScrambleMetaStore(conn, options);
      return new VerdictResultStreamFromSingleResult(metaStore.showScrambles());

    } else if (queryType.equals(QueryType.set_default_schema)) {
      log.debug("Query type: set_default_schema");
      updateDefaultSchemaFromQuery(query);
      return null;

    } else if (queryType.equals(QueryType.show_databases)) {
      log.debug("Query type: show_databases");
      VerdictResultStream stream = generateShowSchemaResultFromQuery();
      return stream;

    } else if (queryType.equals(QueryType.show_tables)) {
      log.debug("Query type: show_tables");
      VerdictResultStream stream = generateShowTablesResultFromQuery(query);
      return stream;

    } else if (queryType.equals(QueryType.describe_table)) {
      log.debug("Query type: describe_table");
      VerdictResultStream stream = generateDescribeTableResultFromQuery(query);
      return stream;

    } else {
      throw new VerdictDBTypeException("Unexpected type of query: " + query);
    }
  }

  private void refreshScrambleMetaStore() {
    // no type check was added to make it fail if non-cached metastore is used.
    ((CachedScrambleMetaStore) this.metaStore).refreshCache();
  }

  private Pair<BaseTable, BaseTable> getTablePairForDropScramble(String query) {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    VerdictSQLParserBaseVisitor<Pair<BaseTable, BaseTable>> visitor = new VerdictSQLParserBaseVisitor<Pair<BaseTable, BaseTable>>() {
      @Override
      public Pair<BaseTable, BaseTable> visitDrop_scramble_statement(
          VerdictSQLParser.Drop_scramble_statementContext ctx) {
        RelationGen g = new RelationGen();
        BaseTable originalTable = (BaseTable) g.visit(ctx.original_table);
        BaseTable scrambleTable = (BaseTable) g.visit(ctx.scrambled_table);
        return ImmutablePair.of(originalTable, scrambleTable);
      }
    };
    return visitor.visit(parser.drop_scramble_statement());
  }

  private BaseTable getTableForDropAllScramble(String query) {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    VerdictSQLParserBaseVisitor<BaseTable> visitor = new VerdictSQLParserBaseVisitor<BaseTable>() {
      @Override
      public BaseTable visitDrop_all_scrambles_statement(
          VerdictSQLParser.Drop_all_scrambles_statementContext ctx) {
        RelationGen g = new RelationGen();
        return (BaseTable) g.visit(ctx.original_table);
      }
    };
    return visitor.visit(parser.drop_all_scrambles_statement());
  }

  private String stripQuote(String expr) {
    if (expr == null) {
      return null;
    }
    return expr.replace("\"", "").replace("`", "").replace("'", "");
  }

  private CreateScrambleQuery generateScrambleQuery(String query) {

    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    VerdictSQLParserBaseVisitor<CreateScrambleQuery> visitor = new VerdictSQLParserBaseVisitor<CreateScrambleQuery>() {
      @Override
      public CreateScrambleQuery visitCreate_scramble_statement(
          VerdictSQLParser.Create_scramble_statementContext ctx) {
        RelationGen g = new RelationGen();
        BaseTable originalTable = (BaseTable) g.visit(ctx.original_table);
        BaseTable scrambleTable = (BaseTable) g.visit(ctx.scrambled_table);
        String method = (ctx.method == null) ? "uniform"
            : stripQuote(ctx.method.getText());
        double percent = (ctx.percent == null) ? 1.0 : Double.parseDouble(ctx.percent.getText());
        long blocksize = 
            (ctx.blocksize == null) ? (long) 1e6 : Long.parseLong(ctx.blocksize.getText());
        String hashColumnName = 
            (ctx.hash_column == null) ? null : stripQuote(ctx.hash_column.getText());

        CreateScrambleQuery query = new CreateScrambleQuery(scrambleTable.getSchemaName(),
            scrambleTable.getTableName(), originalTable.getSchemaName(), originalTable.getTableName(), method,
            percent, blocksize, hashColumnName);
        if (ctx.IF() != null)
          query.setIfNotExists(true);
        return query;
      }
    };

    CreateScrambleQuery scrambleQuery = visitor.visit(parser.create_scramble_statement());
    return scrambleQuery;
  }

  private VerdictResultStream generateShowSchemaResultFromQuery() throws VerdictDBException {
    List<String> header = Arrays.asList("schema");
    List<String> rows = conn.getSchemas();
    VerdictSingleResultFromListData result = createWithSingleColumn(header, (List) rows);
    return new VerdictResultStreamFromSingleResult(result);
  }

  private VerdictResultStream generateShowTablesResultFromQuery(String query) throws VerdictDBException {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    IdContext schemaCtx = parser.show_tables_statement().schema;
    String schema = (schemaCtx == null)? 
        conn.getDefaultSchema()
        : schemaCtx.getText();
    List<String> header = Arrays.asList("table");
    List<String> rows = conn.getTables(schema);
    VerdictSingleResultFromListData result = createWithSingleColumn(header, (List) rows);
    return new VerdictResultStreamFromSingleResult(result);
  }

  private VerdictResultStream generateDescribeTableResultFromQuery(String query) throws VerdictDBException {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    VerdictSQLParserBaseVisitor<Pair<String, String>> visitor = new VerdictSQLParserBaseVisitor<Pair<String, String>>() {
      @Override
      public Pair<String, String> visitDescribe_table_statement(
          VerdictSQLParser.Describe_table_statementContext ctx) {
        IdContext schemaCtx = ctx.table.schema;
        String table = ctx.table.table.getText();
        String schema = (schemaCtx == null)?
            conn.getDefaultSchema()
            : schemaCtx.getText();
//        if (schema == null) {
//          schema = conn.getDefaultSchema();
//        }
        return new ImmutablePair<>(schema, table);
      }
    };
    Pair<String, String> t = visitor.visit(parser.verdict_statement());
    String table = t.getRight();
    String schema = t.getLeft();
    if (schema == null) {
      schema = conn.getDefaultSchema();
    }
    List<Pair<String, String>> columnInfo = conn.getColumns(schema, table);
    List<List<String>> newColumnInfo = new ArrayList<>();
    for (Pair<String, String> pair : columnInfo) {
      newColumnInfo.add(Arrays.asList(pair.getLeft(), pair.getRight()));
    }
    List<String> header = Arrays.asList("column name", "column type");
    VerdictSingleResultFromListData result = new VerdictSingleResultFromListData(header,
        (List) newColumnInfo);
    return new VerdictResultStreamFromSingleResult(result);
  }

  private void updateDefaultSchemaFromQuery(String query) throws VerdictDBDbmsException {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    String schema = parser.use_statement().database.getText();
    conn.setDefaultSchema(schema);
  }

  public void abort() {
    log.debug("Aborts an ExecutionContext: " + this);
    if (runningCoordinator != null) {
      Coordinator c = runningCoordinator;
      runningCoordinator = null;
      c.abort();
    }
  }

  /**
   * Terminates existing threads. The created database tables may still exist for successive uses.
   *
   * <p>
   * This method also removes all temporary tables created by this ExecutionContext.
   */
  public void terminate() {
    abort();

    try {
      String schema = options.getVerdictTempSchemaName();
      String tempTablePrefix = String.format("%s_%s_%d", VerdictOption.getVerdictTempTablePrefix(),
          queryContext.getVerdictContextId(), this.serialNumber);
      List<String> tempTableList = new ArrayList<>();

      List<String> allTempTables = conn.getTables(schema);
      for (String tableName : allTempTables) {
        if (tableName.startsWith(tempTablePrefix)) {
          tempTableList.add(tableName);
        }
      }

      for (String tempTable : tempTableList) {
        conn.execute(String.format("DROP TABLE IF EXISTS %s.%s", schema, tempTable));
      }
    } catch (VerdictDBDbmsException e) {
      e.printStackTrace();
    }
  }

  public static QueryType identifyQueryType(String query) {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);

    VerdictSQLParserBaseVisitor<QueryType> visitor = new VerdictSQLParserBaseVisitor<QueryType>() {

      @Override
      public QueryType visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
        return QueryType.select;
      }

      @Override
      public QueryType visitCreate_scramble_statement(VerdictSQLParser.Create_scramble_statementContext ctx) {
        return QueryType.scrambling;
      }

      @Override
      public QueryType visitDrop_scramble_statement(VerdictSQLParser.Drop_scramble_statementContext ctx) {
        return QueryType.drop_scramble;
      }

      @Override
      public QueryType visitDrop_all_scrambles_statement(
          VerdictSQLParser.Drop_all_scrambles_statementContext ctx) {
        return QueryType.drop_all_scrambles;
      }

      @Override
      public QueryType visitShow_scrambles_statement(VerdictSQLParser.Show_scrambles_statementContext ctx) {
        return QueryType.show_scrambles;
      }

      @Override
      public QueryType visitUse_statement(VerdictSQLParser.Use_statementContext ctx) {
        return QueryType.set_default_schema;
      }

      @Override
      public QueryType visitShow_databases_statement(VerdictSQLParser.Show_databases_statementContext ctx) {
        return QueryType.show_databases;
      }

      @Override
      public QueryType visitShow_tables_statement(VerdictSQLParser.Show_tables_statementContext ctx) {
        return QueryType.show_tables;
      }

      @Override
      public QueryType visitDescribe_table_statement(VerdictSQLParser.Describe_table_statementContext ctx) {
        return QueryType.describe_table;
      }
    };

    QueryType type = visitor.visit(parser.verdict_statement());
    if (type == null) {
      type = QueryType.unknown;
    }
    return type;
  }
}
