package org.verdictdb.core.scrambling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

  double p0 = 0.5;     // max portion for Tier 0; should be configured dynamically in the future

  double p1 = 0.8;     // max portion for Tier 0 + Tier 1; should be configured dynamically in the future

  static final double OUTLIER_STDDEV_MULTIPLIER = 3.09;

  Optional<String> primaryColumnName = Optional.absent();

  private String scratchpadSchemaName;

  List<Double> tier0CumulProbDist = null;

  List<Double> tier1CumulProbDist = null;

  List<Double> tier2CumulProbDist = null;

  private long outlierSize = 0;   // tier 0 size

  private long smallGroupSizeSum = 0;

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
   * Recall that channels 100 and 101 are reserved for column meta and partition meta, respectively.
   * 
   * This method generates up to three nodes, and the token keys set up by those nodes are:
   * 1. queryResult: this contains avg, std, and count
   * 2. schemaName, tableName: this is the name of the temporary tables that contains a list of large groups.
   * 3. queryResult: this contains the sum of the sizes of large groups.
   */
  @Override
  public List<ExecutableNodeBase> getStatisticsNode(
      String oldSchemaName, String oldTableName, String columnMetaTokenKey, String partitionMetaTokenKey) {

    List<ExecutableNodeBase> statisticsNodes = new ArrayList<>();

    // outlier checking
    PercentilesAndCountNode pc = new PercentilesAndCountNode(
        oldSchemaName, oldTableName, columnMetaTokenKey, partitionMetaTokenKey);
    statisticsNodes.add(pc);

    // outlier proportion computation
    OutlierProportionNode op = new OutlierProportionNode(oldSchemaName, oldTableName);
    op.subscribeTo(pc);
    statisticsNodes.add(op);

    // primary group's distribution checking
    if (primaryColumnName.isPresent()) {
      TempIdCreatorInScratchpadSchema idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
      LargeGroupListNode ll = new LargeGroupListNode(
          idCreator, oldSchemaName, oldTableName, primaryColumnName.get());
      ll.subscribeTo(pc, 0);    // subscribed to 'pc' to obtain count(*) of the table, which is used to infer appropriate sampling ratio.

      LargeGroupSizeNode ls = new LargeGroupSizeNode(primaryColumnName.get());
      ls.subscribeTo(ll, 0);

      statisticsNodes.add(ll);
      statisticsNodes.add(ls);
    }

    return statisticsNodes;
  }

  //  @Override
  //  public int getBlockSize() {
  //    return blockSize;
  //  }

  static UnnamedColumn createOutlierTuplePredicate(DbmsQueryResult percentileAndCountResult) {
    UnnamedColumn outlierPredicate = null;

    percentileAndCountResult.rewind();
    percentileAndCountResult.next();    // assumes that the original table has at least one row.

    for (int i = 0; i < percentileAndCountResult.getColumnCount(); i++) {
      String columnName = percentileAndCountResult.getColumnName(i);
      if (columnName.startsWith(PercentilesAndCountNode.AVG_PREFIX)) {
        String originalColumnName = columnName.substring(PercentilesAndCountNode.AVG_PREFIX.length());

        // this implementation assumes that corresponding stddev column comes right after
        // the current column indexed by 'i'.
        double columnAverage = percentileAndCountResult.getDouble(i);
        double columnStddev = percentileAndCountResult.getDouble(i+1);
        double lowCriteria = columnAverage - columnStddev * OUTLIER_STDDEV_MULTIPLIER;
        double highCriteria = columnAverage + columnStddev * OUTLIER_STDDEV_MULTIPLIER;

        UnnamedColumn newOrPredicate = ColumnOp.or(
            ColumnOp.less(new BaseColumn(originalColumnName), ConstantColumn.valueOf(lowCriteria)),
            ColumnOp.greater(new BaseColumn(originalColumnName), ConstantColumn.valueOf(highCriteria)));

        if (outlierPredicate == null) {
          outlierPredicate = newOrPredicate;
        } else {
          outlierPredicate = ColumnOp.or(outlierPredicate, newOrPredicate);
        }
      }
      if (columnName.equals(PercentilesAndCountNode.TOTAL_COUNT_ALIAS_NAME)) {
        // do nothing
      }
    }

    return outlierPredicate;
  }

  @Override
  public List<UnnamedColumn> getTierExpressions(Map<String, Object> metaData) {
    DbmsQueryResult percentileAndCountResult = (DbmsQueryResult) metaData.get("0queryResult");
    //    String largeGroupListSchemaName = (String) metaData.get("1schemaName");
    //    String largeGroupListTableName = (String) metaData.get("1tableName");

    // Tier 0
    UnnamedColumn tier0Predicate = createOutlierTuplePredicate(percentileAndCountResult);

    // Tier 1
    // select (case ... when t2.groupSize is null then 1 else 2 end) as verdictdbtier
    // from schemaName.tableName t1 left join largeGroupSchema.largeGroupTable t2
    //   on t1.primaryGroup = t2.primaryGroup
    String rightTableSourceAlias = ScramblingNode.RIGHT_TABLE_SOURCE_ALIAS_NAME;
    UnnamedColumn tier1Predicate = ColumnOp.isnull(
        new BaseColumn(rightTableSourceAlias, LargeGroupListNode.LARGE_GROUP_SIZE_COLUMN_ALIAS));

    // Tier 2: automatically handled by this function's caller

    return Arrays.asList(tier0Predicate, tier1Predicate);
  }

  @Override
  public List<Double> getCumulativeProbabilityDistributionForTier(Map<String, Object> metaData, int tier) {
    // this cumulative prob is calculated at the first call of this method
    if (tier0CumulProbDist == null) {
      populateAllCumulativeProbabilityDistribution(metaData);
    }

    if (tier == 0) {
      return tier0CumulProbDist;
    } else if (tier == 1) {
      return tier1CumulProbDist;
    } else {
      // expected tier == 2
      return tier2CumulProbDist;
    }
  }

  private void populateAllCumulativeProbabilityDistribution(Map<String, Object> metaData) {
    populateTier0CumulProbDist(metaData);
    populateTier1CumulProbDist(metaData);
    populateTier2CumulProbDist(metaData);
  }
  
  private void populateTier0CumulProbDist(Map<String, Object> metaData) {
    List<Double> cumulProbDist = new ArrayList<>();

    // calculate the number of blocks
    Pair<Long, Long> tableSizeAndBlockNumber = retrieveTableSizeAndBlockNumber(metaData);
    long tableSize = tableSizeAndBlockNumber.getLeft();
    long totalNumberOfblocks = tableSizeAndBlockNumber.getRight();

    DbmsQueryResult outlierProportion = (DbmsQueryResult) metaData.get("1resultSet");
    outlierSize = outlierProportion.getLong(0);

    if (outlierSize * 2 >= tableSize) {
      // too large outlier -> no special treatment
      for (int i = 0; i < totalNumberOfblocks; i++) {
        cumulProbDist.add((i+1) / (double) totalNumberOfblocks);
      }
      
    } else {
      Long remainingSize = outlierSize;

      while (remainingSize > 0) {  
        // fill only p0 portion of each block at most
        if (remainingSize <= p0 * blockSize) {
          cumulProbDist.add(1.0);
          break;
        } else {
          long thisBlockSize = (long) (blockSize * p0);
          double ratio = thisBlockSize / outlierSize;
          if (cumulProbDist.size() == 0) {
            cumulProbDist.add(ratio);
          } else {
            cumulProbDist.add(cumulProbDist.get(cumulProbDist.size()-1) + ratio);
          }
          remainingSize -= thisBlockSize;
        }
      }

      // in case where the length of the prob distribution is not equal to the total block number
      while (cumulProbDist.size() < totalNumberOfblocks) {
        cumulProbDist.add(1.0);
      }
    }

    tier0CumulProbDist = cumulProbDist;
  }

  /**
   * Probability distribution for Tier1: small-group tier
   * @param metaData
   */
  private void populateTier1CumulProbDist(Map<String, Object> metaData) {
    List<Double> cumulProbDist = new ArrayList<>();

    // calculate the number of blocks
    Pair<Long, Long> tableSizeAndBlockNumber = retrieveTableSizeAndBlockNumber(metaData);
    long tableSize = tableSizeAndBlockNumber.getLeft();
    long totalNumberOfblocks = tableSizeAndBlockNumber.getRight();
    
    // obtain total large group size
    DbmsQueryResult largeGroupSizeResult = (DbmsQueryResult) metaData.get("3queryResult");
    largeGroupSizeResult.rewind();
    largeGroupSizeResult.next();
    long largeGroupSizeSum = largeGroupSizeResult.getLong(0);
    smallGroupSizeSum = tableSize - largeGroupSizeSum;
    
    // count the remaining size (after tier0)
    long totalRemainingSizeUnderP1 = 0;
    for (int i = 0; i < tier0CumulProbDist.size(); i++) {
      if (i == 0) {
        totalRemainingSizeUnderP1 += blockSize * p1 - outlierSize * tier0CumulProbDist.get(i);
      } else {
        totalRemainingSizeUnderP1 += blockSize * p1 - outlierSize * (tier0CumulProbDist.get(i+1) - tier0CumulProbDist.get(i));
      }
    }
    
    // In this extreme case (i.e., the sum of the sizes of small groups is too big),
    // we simply uniformly distribute them.
    if (smallGroupSizeSum >= totalRemainingSizeUnderP1) {
      for (int i = 0; i < totalNumberOfblocks; i++) {
        cumulProbDist.add((i+1) / (double) totalNumberOfblocks);
      }
    } 
    // Otherwise, we fill up to (p1 - p0) portion within each block.
    else {
      long remainingSize = smallGroupSizeSum;
      
      while (remainingSize > 0) {
        if (remainingSize <= (p1 - p0) * blockSize) {
          cumulProbDist.add(1.0);
          break;
        } else {
          long thisBlockSize = (long) (blockSize * (p1 - p0));
          double ratio = thisBlockSize / smallGroupSizeSum;
          if (cumulProbDist.size() == 0) {
            cumulProbDist.add(ratio);
          } else {
            cumulProbDist.add(cumulProbDist.get(cumulProbDist.size()-1) + ratio);
          }
          remainingSize -= thisBlockSize;
        }
      }
      
      // in case where the length of the prob distribution is not equal to the total block number
      while (cumulProbDist.size() < totalNumberOfblocks) {
        cumulProbDist.add(1.0);
      }
    }

    tier1CumulProbDist = cumulProbDist;
  }
  
  /**
   * Probability distribution for Tier2: the tuples that do not belong to tier0 or tier1.
   * @param metaData
   */
  private void populateTier2CumulProbDist(Map<String, Object> metaData) {
    List<Double> cumulProbDist = new ArrayList<>();
    
    // calculate the number of blocks
    Pair<Long, Long> tableSizeAndBlockNumber = retrieveTableSizeAndBlockNumber(metaData);
    long tableSize = tableSizeAndBlockNumber.getLeft();
    long totalNumberOfblocks = tableSizeAndBlockNumber.getRight();
    
    long tier2Size = tableSize - outlierSize - smallGroupSizeSum;
    
    for (int i = 0; i < totalNumberOfblocks; i++) {
      long thisTier0Size;
      long thisTier1Size;
      if (i == 0) {
        thisTier0Size = (long) (outlierSize * tier0CumulProbDist.get(i));
        thisTier1Size = (long) (smallGroupSizeSum * tier1CumulProbDist.get(i));
      } else {
        thisTier0Size = (long) (outlierSize * (tier0CumulProbDist.get(i) - tier0CumulProbDist.get(i-1)));
        thisTier1Size = (long) (smallGroupSizeSum * (tier1CumulProbDist.get(i) - tier1CumulProbDist.get(i-1)));
      }
      long thisBlockSize = blockSize -  thisTier0Size - thisTier1Size;
      double thisBlockRatio = thisBlockSize / tier2Size;
      cumulProbDist.add(thisBlockRatio);
    }
    
    tier2CumulProbDist = cumulProbDist;
  }
  
  // Helper
  private Pair<Long, Long> retrieveTableSizeAndBlockNumber(Map<String, Object> metaData) {
    DbmsQueryResult tableSizeResult = (DbmsQueryResult) metaData.get("0queryResult");
    tableSizeResult.rewind();
    tableSizeResult.next();
    long tableSize = tableSizeResult.getLong(0);
    long totalNumberOfblocks = (long) Math.ceil(tableSize / (float) blockSize);
    return Pair.of(tableSize, totalNumberOfblocks);
  }

}

class PercentilesAndCountNode extends QueryNodeBase {

  private String schemaName;

  private String tableName;

  private String columnMetaTokenKey;

  private String partitionMetaTokenKey;

  public static final String AVG_PREFIX = "verdictdbavg";

  public static final String STDDEV_PREFIX = "verdictdbstddev";

  public static final String TOTAL_COUNT_ALIAS_NAME = "verdictdbtotalcount";

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
          AVG_PREFIX + col);
      selectList.add(item);

      // standard deviation
      item = new AliasedColumn(
          ColumnOp.std(new BaseColumn(tableSourceAlias, col)), 
          STDDEV_PREFIX + col);
      selectList.add(item);
    }
    selectList.add(new AliasedColumn(ColumnOp.count(), TOTAL_COUNT_ALIAS_NAME));

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


/**
 * select count(*)
 * from originalSchema.originalTable
 * where some-outlier-predicates
 * 
 * @author Yongjoo Park
 *
 */
class OutlierProportionNode extends QueryNodeBase {

  private String schemaName;

  private String tableName;

  public static String OUTLIER_SIZE_ALIAS = "verdictdbOutlierProportion";

  public OutlierProportionNode(String schemaName, String tableName) {
    super(null);
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    DbmsQueryResult percentileAndCountResult = (DbmsQueryResult) tokens.get(0).getValue("queryResult");
    UnnamedColumn outlierPrediacte = FastConvergeScramblingMethod.createOutlierTuplePredicate(percentileAndCountResult);
    String tableSourceAliasName = "t";

    selectQuery = SelectQuery.create(
        new AliasedColumn(ColumnOp.count(), OUTLIER_SIZE_ALIAS),
        new BaseTable(schemaName, tableName, tableSourceAliasName));
    selectQuery.addFilterByAnd(outlierPrediacte);

    return selectQuery;

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
   * create table some-temp-table-name as
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
