package org.verdictdb.core.scrambling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DataTypeConverter;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.querying.CreateTableAsSelectNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.IdCreator;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.querying.QueryNodeWithPlaceHolders;
import org.verdictdb.core.querying.SubscriptionTicket;
import org.verdictdb.core.querying.TempIdCreatorInScratchpadSchema;
import org.verdictdb.core.sqlobject.AliasReference;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

import com.google.common.base.Optional;

/**
 * 
 * Policy:
 * 1. Tier 0: tuples containing outlier values.
 * 2. Tier 1: tuples containing rare groups
 * 3. Tier 2: other tuples
 * 
 * Outlier values:
 * Bottom 0.1% and 99.9% percentile values for every numeric column. Suppose a table has 20 numeric columns; 
 * then, in the worst case, 20 * 0.2% = 4% of the tuples are the tuples containing outlier values.
 * 
 * Blocks for Tier 0:
 * Tier 0 takes up to 50% of each block.
 * 
 * Blocks for Tier 1:
 * Tier 0 + Tier 1 take up to 80% of each block.
 * 
 * Blocks for Tier 2:
 * Tier 2 takes the rest of the space in each block.
 * 
 * @author Yongjoo Park
 *
 */
public class FastConvergeScramblingMethod extends ScramblingMethodBase {

  final double p0 = 0.5;     // max portion for Tier 0; should be configured dynamically in the future

  final double p1 = 0.8;     // max portion for Tier 0 + Tier 1; should be configured dynamically in the future

  Optional<String> primaryColumnName = Optional.absent();
  
  private String scratchpadSchemaName;

  public FastConvergeScramblingMethod(long blockSize, String scratchpadSchemaName) {
    super(blockSize);
    this.scratchpadSchemaName = scratchpadSchemaName;
  }

  public FastConvergeScramblingMethod(long blockSize, String scratchpadSchemaName, String primaryColumnName) {
    this(blockSize, scratchpadSchemaName);
    this.primaryColumnName = Optional.of(primaryColumnName);
  }

  /**
   * Computes three nodes. They compute: 
   * (1) 0.1% and 99.9% percentiles of numeric columns and the total count,
   *     (for this, we compute standard deviations and estimate those percentiles based on the standard
   *      deviations and normal distribution assumptions. \pm 3.09 * stddev is the 99.9 and 0.1 percentiles
   *      of the standard normal distribution.)
   * (2) the list of "large" groups, and 
   * (3) the sizes of "large" groups
   * 
   * Recall that channels 0 and 1 are reserved for column meta and partition meta, respectively.
   * 
   */
  @Override
  public List<ExecutableNodeBase> getStatisticsNode(
      String oldSchemaName, String oldTableName, String columnMetaTokenKey, String partitionMetaTokenKey) {
    
    List<ExecutableNodeBase> statisticsNodes = new ArrayList<>();

    // outlier checking
    PercentilesAndCountNode pc = new PercentilesAndCountNode(
        oldSchemaName, oldTableName, columnMetaTokenKey, partitionMetaTokenKey);
    statisticsNodes.add(pc);

    // primary group's distribution checking
    if (primaryColumnName.isPresent()) {
      TempIdCreatorInScratchpadSchema idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
      LargeGroupListNode ll = new LargeGroupListNode(
          idCreator, oldSchemaName, oldTableName, primaryColumnName.get());
  
      LargeGroupSizeNode ls = new LargeGroupSizeNode(primaryColumnName.get());
      ls.subscribeTo(ll, 2);
      
      statisticsNodes.add(ll);
      statisticsNodes.add(ls);
    }

    return statisticsNodes;
  }

  @Override
  public int getBlockCount(DbmsQueryResult statistics) {
    // TODO: will use the provided statistics
    return 10;
  }

  @Override
  public List<UnnamedColumn> getTierExpressions(DbmsQueryResult statistics) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Double> getCumulativeProbabilityDistributionForTier(
      DbmsQueryResult statistics, 
      int tier, 
      int length) {
    // TODO Auto-generated method stub
    return null;
  }

}

class PercentilesAndCountNode extends QueryNodeBase {

  private String schemaName;

  private String tableName;

  private String columnMetaTokenKey;

  private String partitionMetaTokenKey;

  public PercentilesAndCountNode(
      String schemaName, String tableName, String columnMetaTokenKey, String partitionMetaTokenKey) {
    super(null);
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.columnMetaTokenKey = columnMetaTokenKey;
    this.partitionMetaTokenKey = partitionMetaTokenKey;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    if (tokens.size() == 0) {
      // no token information passed
      throw new VerdictDBValueException("No token is passed.");
    }
    
    @SuppressWarnings("unchecked")
    List<Pair<String, String>> columnNameAndTypes = 
    (List<Pair<String, String>>) tokens.get(0).getValue(columnMetaTokenKey);
    
    if (columnNameAndTypes == null) {
      throw new VerdictDBValueException("The passed token does not have the key: " + columnMetaTokenKey);
    }
    
    List<String> numericColumns = getNumericColumns(columnNameAndTypes);
    String tableSourceAlias = "t";

    // compose a select list
    List<SelectItem> selectList = new ArrayList<>();
    for (String col : numericColumns) {
      // mean 
      SelectItem item;
      item = new AliasedColumn(
          ColumnOp.avg(new BaseColumn(tableSourceAlias, col)), 
          col + "avg");
      selectList.add(item);

      // standard deviation
      item = new AliasedColumn(
          ColumnOp.std(new BaseColumn(tableSourceAlias, col)), 
          col + "std");
      selectList.add(item);
    }
    selectList.add(new AliasedColumn(ColumnOp.count(), "total"));

    selectQuery = SelectQuery.create(
        selectList, 
        new BaseTable(schemaName, tableName, tableSourceAlias));
    return selectQuery;
  }

  private List<String> getNumericColumns(List<Pair<String, String>> columnNameAndTypes) {
    List<String> numericColumns = new ArrayList<>();
    for (Pair<String, String> nameAndType : columnNameAndTypes) {
      String name = nameAndType.getLeft();
      String type = nameAndType.getRight();
      if (DataTypeConverter.isNumeric(type)) {
        numericColumns.add(name);
      }
    }
    return numericColumns;
  }

}


class LargeGroupListNode extends CreateTableAsSelectNode {
  
  // TODO: how to tweak this value?
  private final double p0 = 0.001;

  private String schemaName;

  private String tableName;

  private String primaryColumnName;
  
  public static final String LARGE_GROUP_SIZE_COLUMN_ALIAS = "groupSize";

  public LargeGroupListNode(
      IdCreator idCreator, String schemaName, String tableName, String primaryColumnName) {
    super(idCreator, null);
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.primaryColumnName = primaryColumnName;
  }

  /**
   * select primaryGroup, count(*) * (1/p0)
   * from schemaName.tableName
   * where rand() < p0
   * group by primaryGroup;
   * @throws VerdictDBException 
   * 
   */
  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    String tableSourceAlias = "t";
    
    // select
    List<SelectItem> selectList = new ArrayList<>();
    selectList.add(new BaseColumn(tableSourceAlias, primaryColumnName));
    selectList.add(new AliasedColumn(
        ColumnOp.multiply(
            ColumnOp.count(), 
            ColumnOp.divide(ConstantColumn.valueOf(1.0), ConstantColumn.valueOf(p0))), 
        LARGE_GROUP_SIZE_COLUMN_ALIAS));
    
    // from
    SelectQuery selectQuery = SelectQuery.create(
        selectList,
        new BaseTable(schemaName, tableName, tableSourceAlias));
    
    // where
    selectQuery.addFilterByAnd(ColumnOp.less(ColumnOp.rand(), ConstantColumn.valueOf(p0)));
    
    // group by
    selectQuery.addGroupby(new AliasReference(primaryColumnName));
    
    this.selectQuery = selectQuery;
    return super.createQuery(tokens);
  }
  
}


class LargeGroupSizeNode extends QueryNodeWithPlaceHolders {

  private String primaryColumnName;
  
  public static final String LARGE_GROUP_SIZE_SUM_ALIAS = "largeGroupSizeSum";

  public LargeGroupSizeNode(String primaryColumnName) {
    super(null);
    this.primaryColumnName = primaryColumnName;
  }
  
  /**
   * select count(*) as totalLargeGroupSize
   * from verdicttemptable;
   * 
   * @param tokens
   * @return
   * @throws VerdictDBException
   */
  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    String tableSourceAlias = "t";
    String aliasName = LARGE_GROUP_SIZE_SUM_ALIAS;
    String groupSizeAlias = LargeGroupListNode.LARGE_GROUP_SIZE_COLUMN_ALIAS;
    
    // Note: this node already has been subscribed; thus, we don't need an explicit subscription.
    Pair<BaseTable, SubscriptionTicket> placeholder = createPlaceHolderTable(tableSourceAlias);
    BaseTable baseTable = placeholder.getLeft();
    selectQuery = SelectQuery.create(
        new AliasedColumn(
            ColumnOp.sum(new BaseColumn(tableSourceAlias, groupSizeAlias)), 
            aliasName),
        baseTable);
    
    super.createQuery(tokens);      // placeholder replacements performed here
    return selectQuery;
  }

}


/**
 * Retrieves the following information.
 * 
 * 1. Top and bottom 0.1% percentile values of every numeric column.
 * 2. Total count
 * 
 * @author Yongjoo Park
 *
 */
class FastConvergeStatisticsQueryGenerator implements StatiticsQueryGenerator {

  @Override
  public SelectQuery create(
      String schemaName, 
      String tableName, 
      List<Pair<String, String>> columnNamesAndTypes, 
      List<String> partitionColumnNames) {


    // TODO Auto-generated method stub
    return null;
  }

}
