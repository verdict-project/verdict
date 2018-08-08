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
import org.verdictdb.VerdictContext;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.CreateScrambleQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.metastore.ScrambleMetaStore;
import org.verdictdb.parser.VerdictSQLParser;
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

  private VerdictContext context;

  private ScrambleMetaStore metaStore;

  private final long serialNumber;

  private static final VerdictDBLogger LOG = VerdictDBLogger.getLogger(ExecutionContext.class);

  private enum QueryType {
    select,
    scrambling,
    set_default_schema,
    unknown,
    show_databases,
    show_tables,
    describe_table
  }

  /**
   * @param context Parent context
   * @param serialNumber
   */
  public ExecutionContext(VerdictContext context, long serialNumber) {
    this.context = context;
    this.serialNumber = serialNumber;
    this.metaStore = new ScrambleMetaStore(context.getCopiedConnection());
  }

  public long getExecutionContextSerialNumber() {
    return serialNumber;
  }

  public VerdictSingleResult sql(String query) throws VerdictDBException {
    VerdictResultStream stream = streamsql(query);
    if (stream == null) {
      return null;
    }

    VerdictSingleResult result = stream.next();
    stream.close();
    return result;
  }

  public VerdictResultStream streamsql(String query) throws VerdictDBException {
    // determines the type of the given query and forward it to an appropriate coordinator.

    QueryType queryType = identifyQueryType(query);

    if (queryType.equals(QueryType.select)) {
      LOG.debug("Query type: select");
      SelectQueryCoordinator coordinator =
          new SelectQueryCoordinator(context.getCopiedConnection());
      coordinator.setScrambleMetaSet(metaStore.retrieve());
      ExecutionResultReader reader = coordinator.process(query);
      VerdictResultStream stream = new VerdictResultStreamFromExecutionResultReader(reader, this);
      return stream;
      
    } else if (queryType.equals(QueryType.scrambling)) {
      LOG.debug("Query type: scrambling");
      CreateScrambleQuery scrambleQuery = generateScrambleQuery(query);
      ScramblingCoordinator scrambler =
          new ScramblingCoordinator(context.getCopiedConnection(), scrambleQuery.getNewSchema());

      // Specifying size/ratio of scrambled table is not supported.
      if (scrambleQuery.getSize() != 1.0) {
        throw new VerdictDBTypeException(
            String.format("Scramble size of %f not supported.", scrambleQuery.getSize()));
      }

      // store this to our own metadata db.
      ScrambleMeta meta = scrambler.scramble(scrambleQuery);
      //              scrambleQuery.getOriginalSchema(),
      //              scrambleQuery.getOriginalTable(),
      //              scrambleQuery.getNewSchema(),
      //              scrambleQuery.getNewTable(),
      //              scrambleQuery.getMethod()); // dyoon: size is not used atm?

      // Add metadata to metastore
      metaStore.addToStore(meta);
      return null;
      
    } else if (queryType.equals(QueryType.set_default_schema)) {
      LOG.debug("Query type: set_default_schema");
      updateDefaultSchemaFromQuery(query);
      return null;
    } else if (queryType.equals(QueryType.show_databases)) {
      LOG.debug("Query type: show_databases");
      VerdictResultStream stream = generateShowSchemaResultFromQuery();
      return stream;
    } else if (queryType.equals(QueryType.show_tables)) {
      LOG.debug("Query type: show_tables");
      VerdictResultStream stream = generateShowTablesResultFromQuery(query);
      return stream;
    } else if (queryType.equals(QueryType.describe_table)) {
      LOG.debug("Query type: describe_table");
      VerdictResultStream stream = generateDesribeTableResultFromQuery(query);
      return stream;
    } else {
      throw new VerdictDBTypeException("Unexpected type of query: " + query);
    }
  }

  private CreateScrambleQuery generateScrambleQuery(String query) {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    VerdictSQLParserBaseVisitor<CreateScrambleQuery> visitor =
        new VerdictSQLParserBaseVisitor<CreateScrambleQuery>() {
          @Override
          public CreateScrambleQuery visitCreate_scramble_statement(
              VerdictSQLParser.Create_scramble_statementContext ctx) {
            RelationGen g = new RelationGen();
            BaseTable originalTable = (BaseTable) g.visit(ctx.original_table);
            BaseTable scrambleTable = (BaseTable) g.visit(ctx.scrambled_table);
            String method =
                (ctx.scrambling_method_name() == null)
                    ? "uniform"
                    : ctx.scrambling_method_name()
                        .getText()
                        .replace("'", "")
                        .replace("\"", "")
                        .replace("`", ""); // remove all types of 'quotes'
            double percent =
                (ctx.percent == null) ? 1.0 : Double.parseDouble(ctx.percent.getText());
            CreateScrambleQuery query =
                new CreateScrambleQuery(
                    scrambleTable.getSchemaName(),
                    scrambleTable.getTableName(),
                    originalTable.getSchemaName(),
                    originalTable.getTableName(),
                    method,
                    percent);
            if (ctx.IF() != null) query.setIfNotExists(true);
            return query;
          }
        };

    CreateScrambleQuery scrambleQuery = visitor.visit(parser.create_scramble_statement());
    return scrambleQuery;
  }

  private VerdictResultStream generateShowSchemaResultFromQuery() throws VerdictDBException {
    List<String> header = Arrays.asList("schema");
    List<String> rows = context.getConnection().getSchemas();
    VerdictSingleResultFromListData result = createWithSingleColumn(header, (List) rows);
    return new VerdictResultStreamFromSingleResult(result);
  }

  private VerdictResultStream generateShowTablesResultFromQuery(String query)
      throws VerdictDBException {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    String schema = parser.show_tables_statement().schema.getText();
    List<String> header = Arrays.asList("table");
    List<String> rows = context.getConnection().getTables(schema);
    VerdictSingleResultFromListData result = createWithSingleColumn(header, (List) rows);
    return new VerdictResultStreamFromSingleResult(result);
  }

  private VerdictResultStream generateDesribeTableResultFromQuery(String query)
      throws VerdictDBException {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    VerdictSQLParserBaseVisitor<Pair<String, String>> visitor =
        new VerdictSQLParserBaseVisitor<Pair<String, String>>() {
          @Override
          public Pair<String, String> visitDescribe_table_statement(
              VerdictSQLParser.Describe_table_statementContext ctx) {
            String table = ctx.table.table.getText();
            String schema = ctx.table.schema.getText();
            if (schema == null) {
              schema = context.getConnection().getDefaultSchema();
            }
            return new ImmutablePair<>(schema, table);
          }
        };
    Pair<String, String> t = visitor.visit(parser.verdict_statement());
    String table = t.getRight();
    String schema = t.getLeft();
    if (schema == null) {
      schema = context.getConnection().getDefaultSchema();
    }
    List<Pair<String, String>> columnInfo = context.getConnection().getColumns(schema, table);
    List<List<String>> newColumnInfo = new ArrayList<>();
    for (Pair<String, String> pair : columnInfo) {
      newColumnInfo.add(Arrays.asList(pair.getLeft(), pair.getRight()));
    }
    List<String> header = Arrays.asList("column name", "column type");
    VerdictSingleResultFromListData result =
        new VerdictSingleResultFromListData(header, (List) newColumnInfo);
    return new VerdictResultStreamFromSingleResult(result);
  }

  private void updateDefaultSchemaFromQuery(String query) {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    String schema = parser.use_statement().database.getText();
    context.getConnection().setDefaultSchema(schema);
  }

  /**
   * Terminates existing threads. The created database tables may still exist for successive uses.
   */
  public void terminate() {
    // TODO Auto-generated method stub

  }

  private QueryType identifyQueryType(String query) {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);

    VerdictSQLParserBaseVisitor<QueryType> visitor =
        new VerdictSQLParserBaseVisitor<QueryType>() {

          @Override
          public QueryType visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
            return QueryType.select;
          }

          @Override
          public QueryType visitCreate_scramble_statement(
              VerdictSQLParser.Create_scramble_statementContext ctx) {
            return QueryType.scrambling;
          }

          @Override
          public QueryType visitUse_statement(VerdictSQLParser.Use_statementContext ctx) {
            return QueryType.set_default_schema;
          }

          @Override
          public QueryType visitShow_databases_statement(
              VerdictSQLParser.Show_databases_statementContext ctx) {
            return QueryType.show_databases;
          }

          @Override
          public QueryType visitShow_tables_statement(
              VerdictSQLParser.Show_tables_statementContext ctx) {
            return QueryType.show_tables;
          }

          @Override
          public QueryType visitDescribe_table_statement(
              VerdictSQLParser.Describe_table_statementContext ctx) {
            return QueryType.describe_table;
          }
        };

    QueryType type = visitor.visit(parser.verdict_statement());
    return type;
  }
}
