package org.verdictdb.coordinator;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.VerdictContext;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.parser.VerdictSQLParser;
import org.verdictdb.parser.VerdictSQLParser.Create_scramble_statementContext;
import org.verdictdb.parser.VerdictSQLParserBaseVisitor;
import org.verdictdb.sqlreader.NonValidatingSQLParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Stores the context for a single query execution. Includes both scrambling query and select query.
 * 
 * @author Yongjoo Park
 *
 */
public class ExecutionContext {
  
  private VerdictContext context;
  
  private final long serialNumber;
  
  private enum QueryType {
    select, scrambling, set_default_schema, unknown, show_databases, show_tables, describe_table
  }
  
  /**
   * 
   * @param context Parent context
   * @param contextId
   */
  public ExecutionContext(VerdictContext context, long serialNumber) {
    this.context = context;
    this.serialNumber = serialNumber;
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
      SelectQueryCoordinator coordinator = new SelectQueryCoordinator(context.getCopiedConnection());
      ExecutionResultReader reader = coordinator.process(query);
      VerdictResultStream stream = new VerdictResultStreamFromExecutionResultReader(reader, this);
      return stream;
    }
    else if (queryType.equals(QueryType.scrambling)) {
      ScramblingCoordinator coordinator = new ScramblingCoordinator(context.getCopiedConnection());
      return null;
    }
    else if (queryType.equals(QueryType.set_default_schema)) {
      updateDefaultSchemaFromQuery(query);
      return null;
    } else if (queryType.equals(QueryType.show_databases)) {
      return generateShowSchemaResultFromQuery();
    } else if (queryType.equals(QueryType.show_tables)) {
      return generateShowTablesResultFromQuery(query);
    } else if (queryType.equals(QueryType.describe_table)) {
      return generateDesribeTableResultFromQuery(query);
    } else {
      throw new VerdictDBTypeException("Unexpected type of query: " + query);
    }
  }

  private VerdictResultStream generateShowSchemaResultFromQuery() throws VerdictDBException {
    VerdictSingleResultFromListData result = new VerdictSingleResultFromListData((List)context.getConnection().getSchemas());
    return new VerdictResultStreamFromSingleResult(result);
  }

  private VerdictResultStream generateShowTablesResultFromQuery(String query) throws VerdictDBException {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    String schema = parser.show_tables_statement().schema.getText();
    VerdictSingleResultFromListData result = new VerdictSingleResultFromListData(
        (List)context.getConnection().getTables(schema));
    return new VerdictResultStreamFromSingleResult(result);
  }

  private VerdictResultStream generateDesribeTableResultFromQuery(String query) throws VerdictDBException {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    VerdictSQLParserBaseVisitor<Pair<String, String>> visitor = new VerdictSQLParserBaseVisitor<Pair<String, String>>() {
      @Override
      public Pair<String, String> visitDescribe_table_statement(VerdictSQLParser.Describe_table_statementContext ctx) {
        String table = ctx.table.table.getText();
        String schema = ctx.table.schema.getText();
        if (schema==null) {
          schema = context.getConnection().getDefaultSchema();
        }
        return new ImmutablePair<>(schema, table);
      }
    };
    Pair<String, String> t = visitor.visit(parser.verdict_statement());
    String table = t.getRight();
    String schema = t.getLeft();
    if (schema==null) {
      schema = context.getConnection().getDefaultSchema();
    }
    List<Pair<String, String>> columnInfo = context.getConnection().getColumns(schema, table);
    List<List<String>> newColumnInfo = new ArrayList<>();
    for (Pair<String, String> pair:columnInfo) {
      newColumnInfo.add(Arrays.asList(pair.getLeft(), pair.getRight()));
    }
    VerdictSingleResultFromListData result = new VerdictSingleResultFromListData(
        (List)newColumnInfo);
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
    return type;
  }

}
