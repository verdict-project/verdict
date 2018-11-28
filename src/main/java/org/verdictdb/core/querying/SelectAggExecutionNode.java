package org.verdictdb.core.querying;

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.ola.InMemoryAggregate;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

/**
 *
 * When the outer query is an aggregation query that contains scramble tables,
 * SelectAggExecutionNode will created as the source of SelectAsyncAggExecutionNode.
 * When SelectAggExecutionNode is executed, it will create a temporary table in
 * H2 database that stores the results of subquery.
 *
 * @author Shucheng Zhong
 *
 */
public class SelectAggExecutionNode extends AggExecutionNode {

  private static final long serialVersionUID = 47947858649322912L;
  
  private VerdictDBLogger log;

  private static long selectAggID = 0;

  private final static String IN_MEMORY_TABLE_NAME = "VERDICTDB_SELECTAGG_";

  private InMemoryAggregate inMemoryAggregate;

  public SelectAggExecutionNode(IdCreator idCreator, SelectQuery selectQuery) {
    super(idCreator, selectQuery);
  }

  public static SelectAggExecutionNode create(AggExecutionNode node) {
    SelectAggExecutionNode selectAggExecutionNode = 
        new SelectAggExecutionNode(node.namer, node.selectQuery);
    selectAggExecutionNode.aggMeta = node.aggMeta;
    selectAggExecutionNode.placeholderRecords = node.placeholderRecords;
    selectAggExecutionNode.placeholderTablesinFilter = node.placeholderTablesinFilter;
    for (Pair<ExecutableNodeBase, Integer> pair:node.sources) {
      ExecutableNodeBase child = pair.getLeft();
      selectAggExecutionNode.subscribeTo(child, pair.getRight());
      node.cancelSubscriptionTo(child);
    }
    return selectAggExecutionNode;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery)super.createQuery(tokens);
    return query.getSelect();
  }
  
  private static synchronized String getNextTableName() {
    return IN_MEMORY_TABLE_NAME + selectAggID++;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("aggMeta", aggMeta);
    token.setKeyValue("dependentQuery", this.selectQuery);

    // insert value to in memory database.
    String tableName = getNextTableName();
    try {
      inMemoryAggregate.createTable(result, tableName);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    token.setKeyValue("schemaName", "PUBLIC");
    token.setKeyValue("tableName", tableName);
    return token;
  }

  public void setInMemoryAggregate(InMemoryAggregate inMemoryAggregate) {
    this.inMemoryAggregate = inMemoryAggregate;
  }
}
