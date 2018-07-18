package org.verdictdb.core.querying;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.scrambling.ScramblingMethod;
import org.verdictdb.core.scrambling.ScramblingPlan;
import org.verdictdb.core.sqlobject.CreateScrambledTableQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Dong Young Yoon on 7/17/18.
 */
public class CreateScrambledTableNode extends QueryNodeWithPlaceHolders {

  private static final long serialVersionUID = 1L;

  private IdCreator namer;

  protected String originalSchemaName;

  protected String originalTableName;

  protected String tierColumnName;

  protected String blockColumnName;

  private String newTableSchemaName;

  private String newTableName;

  private List<String> partitionColumns = new ArrayList<>();

  protected ScramblingMethod method;


  public CreateScrambledTableNode(IdCreator namer, SelectQuery query) {
    super(query);
    this.namer = namer;
  }

  public CreateScrambledTableNode(IdCreator namer, SelectQuery query, String originalSchemaName,
                                  String originalTableName, ScramblingMethod method, String tierColumnName,
                                  String blockColumnName) {
    super(query);
    this.namer = namer;
    this.originalSchemaName = originalSchemaName;
    this.originalTableName = originalTableName;
    this.method = method;
    this.tierColumnName = tierColumnName;
    this.blockColumnName = blockColumnName;
  }

  public static CreateScrambledTableNode create(IdCreator namer, SelectQuery query) {
    return new CreateScrambledTableNode(namer, query);
  }

  public IdCreator getNamer() {
    return namer;
  }

  public void setNamer(IdCreator namer) {
    this.namer = namer;
  }

  public void addPartitionColumn(String column) {
    partitionColumns.add(column);
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    super.createQuery(tokens);
    Pair<String, String> tempTableFullName = namer.generateTempTableName();
    newTableSchemaName = tempTableFullName.getLeft();
    newTableName = tempTableFullName.getRight();
    List<Pair<String, String>>  columnMeta = null;
    for (ExecutionInfoToken token : tokens) {
      Object val = token.getValue(ScramblingPlan.COLUMN_METADATA_KEY);
      if (val != null) {
        columnMeta = (List<Pair<String, String>>) val;
        break;
      }
    }
    if (columnMeta == null) {
      throw new VerdictDBException("Column meta is null.");
    }

    CreateScrambledTableQuery createQuery =
        new CreateScrambledTableQuery(originalSchemaName, originalTableName, newTableSchemaName,
            newTableName, tierColumnName, blockColumnName, selectQuery, method.getBlockCount(), columnMeta);
    for (String col : partitionColumns) {
      createQuery.addPartitionColumn(col);
    }
    return createQuery;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("schemaName", newTableSchemaName);
    token.setKeyValue("tableName", newTableName);
    return token;
  }

  @Override
  public ExecutableNodeBase deepcopy() {
    CreateScrambledTableNode node = new CreateScrambledTableNode(namer, selectQuery);
    copyFields(this, node);
    return node;
  }

  void copyFields(CreateScrambledTableNode from, CreateScrambledTableNode to) {
    super.copyFields(from, to);
  }

}
