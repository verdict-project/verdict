package org.verdictdb.connection;

import com.google.common.collect.TreeMultiset;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.AttributeValueRetrievalHelper;
import org.verdictdb.commons.DataTypeConverter;
import org.verdictdb.core.querying.ola.SelectAsyncAggExecutionNode;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.H2Syntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.Date;


public class InMemoryAggregate extends AttributeValueRetrievalHelper implements DbmsQueryResult {

  private static final long serialVersionUID = 2576550919489091L;

  private static final String DB_CONNECTION = "jdbc:h2:mem:verdictdb;DB_CLOSE_DELAY=-1";

  private static final String selectAsyncAggTable = "VERDICTDB_SELECTASYNCAGG";

  private static long selectAsyncAggTableID = 0;

  private static SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new H2Syntax());

  static Connection conn;

  static {
    try {
      conn = DriverManager.getConnection(DB_CONNECTION, "", "");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  // key is the non-aggregate column, value is also HashMap,
  // where the key is the tier value and the value is aggregate columns.
  // This object stores the results from SelectAggExecutionNode (scaled using ScaleFactor)
  private HashMap<List<Object>, HashMap<List<Integer>, List<Double>>> results = new HashMap<>();

  // key is the order-by column, and value is the columns of original selectQuery
  // User fetch data from this object.
  private TreeMultiset<Pair<List<Object>, List<Object>>> rows;

  private List<Object> currentRow;

  private Iterator<Pair<List<Object>, List<Object>>> rowIterator;

  private int cursor = -1;

  // 0 is non-aggregate, 1 is sum or count, 2 is max, 3 is min
  private List<Integer> isAggregate = new ArrayList<>();

  private List<Integer> tierColumnIndex;

  private List<String> aggColumnName;

  private List<String> nonAggColumnName;

  private List<String> columnName;

  // 1 is sum or count, 2 is max, 3 is min
  private List<Integer> isAggregateMaxMin = new ArrayList<>();

  private Integer limit;

  public InMemoryAggregate(SelectAsyncAggExecutionNode node) {
    if (node.getSelectQuery().getLimit().isPresent()) {
      ConstantColumn lim = (ConstantColumn) node.getSelectQuery().getLimit().get();
      if (lim.getValue() instanceof String) {
        limit = Integer.valueOf((String) lim.getValue());
      } else {
        limit = (Integer) lim.getValue();
      }
    }
  }

  public static void createTable(DbmsQueryResult dbmsQueryResult, String tableName) throws SQLException {
    StringBuilder columnNames = new StringBuilder();
    StringBuilder fieldNames = new StringBuilder();
    StringBuilder bindVariables = new StringBuilder();
    for (int i = 0; i < dbmsQueryResult.getColumnCount(); i++) {
      if (i > 0) {
        columnNames.append(", ");
        fieldNames.append(", ");
        bindVariables.append(", ");
      }
      fieldNames.append(dbmsQueryResult.getColumnName(i));
      fieldNames.append(" ");
      fieldNames.append(DataTypeConverter.typeName(dbmsQueryResult.getColumnType(i)));
      columnNames.append(dbmsQueryResult.getColumnName(i));
      bindVariables.append('?');
    }
    // create table
    String createSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + fieldNames + ")";
    conn.createStatement().execute(createSql);

    // insert values
    String sql = "INSERT INTO " + tableName + " ("
        + columnNames
        + ") VALUES ("
        + bindVariables
        + ")";
    PreparedStatement statement = conn.prepareStatement(sql);
    while (dbmsQueryResult.next()) {
      for (int i = 1; i <= dbmsQueryResult.getColumnCount(); i++)
        statement.setObject(i, dbmsQueryResult.getValue(i - 1));
      statement.addBatch();
    }
    statement.executeBatch();
  }

  public static DbmsQueryResult executeQuery(SelectQuery query) throws VerdictDBException, SQLException {
    String sql = selectQueryToSql.toSql(query).toUpperCase();
    ResultSet rs = conn.createStatement().executeQuery(sql);
    return new JdbcQueryResult(rs);
  }

  public static String combine(String combineTableName, String targetTableName, SelectQuery dependentQuery)
      throws SQLException, VerdictDBException {
    String tableName = selectAsyncAggTable + selectAsyncAggTableID++;

    // check targetTable exists
    if (targetTableName.equals("")) {
      // if not just let it be the copy of combineTable
      conn.createStatement().execute(
          String.format("CREATE TABLE %s AS SELECT * FROM %s", tableName, combineTableName));
    } else {
      // if exists, combine two tables using the logic of AggCombinerExecutionNode
      List<GroupingAttribute> groupList = new ArrayList<>();
      SelectQuery copy = dependentQuery.deepcopy();
      for (SelectItem sel : copy.getSelectList()) {
        if (sel instanceof AliasedColumn) {
          UnnamedColumn col = ((AliasedColumn) sel).getColumn();
          resetSchemaAndTableForCombine(col);
          String alias = ((AliasedColumn) sel).getAliasName().toUpperCase();
          ((AliasedColumn) sel).setAliasName(alias);
          if (col.isAggregateColumn()) {
            ((AliasedColumn) sel).setColumn(new ColumnOp("sum", new BaseColumn(alias)));
          } else {
            ((AliasedColumn) sel).setColumn(new BaseColumn(alias));
            groupList.add(((AliasedColumn) sel).getColumn());
          }
        }
      }
      SelectQuery left = SelectQuery.create(new AsteriskColumn(),
          new BaseTable("PUBLIC", targetTableName));
      SelectQuery right = SelectQuery.create(new AsteriskColumn(),
          new BaseTable("PUBLIC", combineTableName));
      AbstractRelation setOperation = new SetOperationRelation(left, right, SetOperationRelation.SetOpType.unionAll);
      copy.clearFilter();
      copy.setFromList(Arrays.asList(setOperation));
      copy.clearGroupby();
      copy.addGroupby(groupList);
      String sql = selectQueryToSql.toSql(copy);
      conn.createStatement().execute(
          String.format("CREATE TABLE %s AS %s", tableName, sql));
    }

    return tableName;
  }

  private static void resetSchemaAndTableForCombine(UnnamedColumn column) {
    List<UnnamedColumn> columns = new ArrayList<>();
    columns.add(column);
    while (!columns.isEmpty()) {
      UnnamedColumn col = columns.get(0);
      columns.remove(0);
      if (col instanceof ColumnOp) {

      }
      if (col instanceof BaseColumn) {
        ((BaseColumn) col).setSchemaName("");
        ((BaseColumn) col).setTableName("UNIONTABLE");
        ((BaseColumn) col).setTableSourceAlias("");
        ((BaseColumn) col).setColumnName(((BaseColumn) col).getColumnName().toUpperCase());
      } else if (col instanceof ColumnOp) {
        columns.addAll(((ColumnOp) col).getOperands());
      }
    }
  }

  public void setIsAggregate(List<Integer> isAggregate) {
    this.isAggregate = isAggregate;
  }

  public void setTierInfo(List<Integer> tierColumnIdx) {
    this.tierColumnIndex = tierColumnIdx;
  }

  /**
   * Once SelectAsyncAggExecutionNode receives a token from SelectAggExecutionNode, it will return call this method
   * to integrate the DbmsQueryResult. It will update itself given scaleFactor. It will do
   * 1. Initialize aggColumnName, nonAggColumnName if not initialized.
   * 2. Use given scaleFactor, first sum up different tier columns of same aggregates and multiply with scale factors.
   * 3. Given the selectQuery of SelectAggExecutionNode, calculate columns by replacing SelectItem with its corresponding values.
   * 4. todo: Generate MetaData if not generated.
   *
   * @param result
   * @param scaleFactor
   * @param node
   */
  public void addDbmsQueryResult(DbmsQueryResult result, HashMap<List<Integer>, Double> scaleFactor, SelectAsyncAggExecutionNode node) {
    HashMap<List<Object>, List<Double>> resultsAfterSumUpTier = new HashMap<>();

    // get column name for agg and non-agg
    if (aggColumnName == null && nonAggColumnName == null) {
      aggColumnName = new ArrayList<>();
      nonAggColumnName = new ArrayList<>();
      for (int i = 0; i < result.getColumnCount(); i++) {
        if (isAggregate.get(i) != 0) {
          aggColumnName.add(result.getColumnName(i));
          isAggregateMaxMin.add(isAggregate.get(i));
        } else if (!tierColumnIndex.contains(i) && !result.getColumnName(i).startsWith("verdictdb_")) {
          nonAggColumnName.add(result.getColumnName(i));
        }
      }
    }

    // update rows
    List<Boolean> isSortedAsc = new ArrayList<>();
    for (OrderbyAttribute orderby : node.getSelectQuery().getOrderby()) {
      if (orderby.getOrder().equals("asc")) {
        isSortedAsc.add(true);
      } else {
        isSortedAsc.add(false);
      }
    }

    rows = TreeMultiset.create(new HashMapComparator(isSortedAsc));

    // append new dbmsQueryResult
    while (result.next()) {
      List<Object> nonAggregate = new ArrayList<>();
      List<Double> aggregate = new ArrayList<>();
      List<Integer> tierValue = new ArrayList<>();
      for (int i = 0; i < tierColumnIndex.size(); i++) {
        tierValue.add(result.getInt(tierColumnIndex.get(i)));
      }
      for (int i = 0; i < result.getColumnCount(); i++) {
        if (isAggregate.get(i) != 0) {
          Object val = result.getValue(i);
          if (val instanceof BigDecimal) {
            aggregate.add(((BigDecimal) val).doubleValue());
          } else if (val instanceof Integer) {
            aggregate.add(Double.valueOf((Integer) val));
          } else if (val instanceof Long) {
            aggregate.add(Double.valueOf((Long) val));
          } else {
            aggregate.add((double) val);
          }
        } else if (!tierColumnIndex.contains(i) && !result.getColumnName(i).startsWith("verdictdb_")) {
          nonAggregate.add(result.getValue(i));
        }
      }
      if (results.containsKey(nonAggregate)) {
        HashMap<List<Integer>, List<Double>> listHashMap = results.get(nonAggregate);
        if (listHashMap.containsKey(tierValue)) {
          List<Double> oldAggregate = listHashMap.get(tierValue);
          for (int i = 0; i < aggregate.size(); i++) {
            if (isAggregateMaxMin.get(i) == 2) {
              aggregate.set(i, aggregate.get(i) > oldAggregate.get(i) ? aggregate.get(i) : oldAggregate.get(i));
            } else if (isAggregateMaxMin.get(i) == 3) {
              aggregate.set(i, aggregate.get(i) < oldAggregate.get(i) ? aggregate.get(i) : oldAggregate.get(i));
            } else {
              aggregate.set(i, aggregate.get(i) + oldAggregate.get(i));
            }
          }
        }
        results.get(nonAggregate).put(tierValue, aggregate);
      } else {
        HashMap<List<Integer>, List<Double>> listHashMap = new HashMap<>();
        listHashMap.put(tierValue, aggregate);
        results.put(nonAggregate, listHashMap);
      }
    }

    // sum up different tiers
    for (Map.Entry<List<Object>, HashMap<List<Integer>, List<Double>>> entry : results.entrySet()) {
      List<Double> sumUpAggregateValues = new ArrayList<>();
      for (Map.Entry<List<Integer>, List<Double>> subentry : entry.getValue().entrySet()) {
        if (sumUpAggregateValues.size() == 0) {
          for (int i = 0; i < subentry.getValue().size(); i++) {
            sumUpAggregateValues.add(scaleFactor.get(subentry.getKey()) * subentry.getValue().get(i));
          }
        } else {
          for (int i = 0; i < subentry.getValue().size(); i++) {
            if (isAggregateMaxMin.get(i) == 2) {
              sumUpAggregateValues.set(i, sumUpAggregateValues.get(i) > scaleFactor.get(subentry.getKey()) * subentry.getValue().get(i)
                  ? sumUpAggregateValues.get(i) : scaleFactor.get(subentry.getKey()) * subentry.getValue().get(i));
            } else if (isAggregateMaxMin.get(i) == 3) {
              sumUpAggregateValues.set(i, sumUpAggregateValues.get(i) < scaleFactor.get(subentry.getKey()) * subentry.getValue().get(i)
                  ? sumUpAggregateValues.get(i) : scaleFactor.get(subentry.getKey()) * subentry.getValue().get(i));
            } else {
              sumUpAggregateValues.set(i, sumUpAggregateValues.get(i) + scaleFactor.get(subentry.getKey()) * subentry.getValue().get(i));
            }
          }
        }
      }
      resultsAfterSumUpTier.put(entry.getKey(), sumUpAggregateValues);
    }

    // get estimated result
    for (Map.Entry<List<Object>, List<Double>> entry : resultsAfterSumUpTier.entrySet()) {
      List<Object> row = new ArrayList<>();
      List<Object> orderbyColumn = new ArrayList<>();
      for (int i = 0; i < node.getSelectQuery().getSelectList().size(); i++) {
        AliasedColumn sel = (AliasedColumn) node.getSelectQuery().getSelectList().get(i);
        Object value;
        if (nonAggColumnName.contains(sel.getAliasName())) {
          // non aggregate
          value = entry.getKey().get(nonAggColumnName.indexOf(sel.getAliasName()));
          row.add(value);
        } else {
          // aggregate
          value = calculateAggregateValue(sel.getColumn(), entry);
          row.add(value);
        }
        for (OrderbyAttribute orderby : node.getSelectQuery().getOrderby()) {
          if (orderby.getAttribute() instanceof AliasReference) {
            if (sel.getAliasName().equals(((AliasReference) orderby.getAttribute()).getAliasName())) {
              orderbyColumn.add(value);
              break;
            } else if (orderby.getAttribute() instanceof UnnamedColumn) {
              if (sel.getColumn().equals(orderby.getAttribute())) {
                orderbyColumn.add(value);
                break;
              }
            }
          }
        }
      }
      rows.add(new ImmutablePair<>(orderbyColumn, row));
    }
    rowIterator = rows.iterator();

    // set up meta data
    if (columnName == null) {
      columnName = new ArrayList<>();
      for (SelectItem sel : node.getSelectQuery().getSelectList()) {
        if (sel instanceof AliasedColumn) {
          columnName.add(((AliasedColumn) sel).getAliasName());
        } else {
          columnName.add(null);
        }
      }
    }
  }

  /**
   * This function will recursively calculate the value of columns given the selectItem.
   * Basically, it will replace all Constant Column and BaseColumn using the value from entry.
   * For ColumnOp, it will do the operation given the opType.
   *
   * @param sel
   * @param entry
   * @return
   */
  private double calculateAggregateValue(SelectItem sel, Map.Entry<List<Object>, List<Double>> entry) {
    if (sel instanceof ColumnOp) {
      ColumnOp columnOp = (ColumnOp) sel;
      if (columnOp.getOpType().equals("sum")) {
        return calculateAggregateValue(columnOp.getOperand(), entry);
      } else if (columnOp.getOpType().equals("add")) {
        return calculateAggregateValue(columnOp.getOperand(0), entry) + calculateAggregateValue(columnOp.getOperand(1), entry);
      } else if (columnOp.getOpType().equals("subtract")) {
        return calculateAggregateValue(columnOp.getOperand(0), entry) - calculateAggregateValue(columnOp.getOperand(1), entry);
      } else if (columnOp.getOpType().equals("multiply")) {
        return calculateAggregateValue(columnOp.getOperand(0), entry) * calculateAggregateValue(columnOp.getOperand(1), entry);
      } else if (columnOp.getOpType().equals("divide")) {
        return calculateAggregateValue(columnOp.getOperand(0), entry) / calculateAggregateValue(columnOp.getOperand(1), entry);
      }
    } else if (sel instanceof BaseColumn) {
      String columnName = ((BaseColumn) sel).getColumnName();
      Object val;
      if (aggColumnName.contains(columnName)) {
        val = entry.getValue().get(aggColumnName.indexOf(columnName));
      } else if (nonAggColumnName.contains(columnName)) {
        val = entry.getKey().get(nonAggColumnName.indexOf(columnName));
      } else {
        val = null;
      }
      if (val instanceof BigDecimal) {
        return ((BigDecimal) val).doubleValue();
      } else {
        return (double) val;
      }
    } else if (sel instanceof ConstantColumn) {
      if (((ConstantColumn) sel).getValue() instanceof String) {
        return Double.valueOf((String) ((ConstantColumn) sel).getValue());
      } else {
        return (double) ((ConstantColumn) sel).getValue();
      }
    }
    return 0;
  }

  @Override
  public DbmsQueryResultMetaData getMetaData() {
    return null;
  }

  @Override
  public int getColumnCount() {
    return columnName.size();
  }

  @Override
  public String getColumnName(int index) {
    return columnName.get(index);
  }

  @Override
  public int getColumnType(int index) {
    return 0;
  }

  @Override
  public void rewind() {
    cursor = -1;
    rowIterator = rows.iterator();
  }

  @Override
  public boolean next() {
    if (rowIterator.hasNext()) {
      currentRow = rowIterator.next().getRight();
      cursor++;
      if (limit != null && cursor >= limit) {
        return false;
      } else {
        return true;
      }
    } else {
      return false;
    }
  }

  @Override
  public long getRowCount() {
    if (limit != null) {
      return limit > rows.size() ? rows.size() : limit;
    }
    return rows.size();
  }

  @Override
  public Object getValue(int index) {
    return currentRow.get(index);
  }

  @Override
  public void printContent() {

  }

  /**
   * Comparator Class to sort the rows.
   */
  class HashMapComparator implements Comparator<Pair<List<Object>, List<Object>>> {

    List<Boolean> isSortedAsc;

    public HashMapComparator(List<Boolean> isSortedAsc) {
      this.isSortedAsc = isSortedAsc;
    }

    private int compareLong(Long v1, Long v2, Boolean isAsc) {
      return isAsc ? v1.compareTo(v2) : v2.compareTo(v1);
    }

    private int compareDouble(Double v1, Double v2, Boolean isAsc) {
      return isAsc ? v1.compareTo(v2) : v2.compareTo(v1);
    }

    private int compareInteger(Integer v1, Integer v2, Boolean isAsc) {
      return isAsc ? v1.compareTo(v2) : v2.compareTo(v1);
    }

    private int compareString(String v1, String v2, Boolean isAsc) {
      return isAsc ? v1.compareTo(v2) : v2.compareTo(v1);
    }

    private int compareDate(Date v1, Date v2, Boolean isAsc) {
      return isAsc ? v1.compareTo(v2) : v2.compareTo(v1);
    }

    private int compareBigDecimal(BigDecimal v1, BigDecimal v2, Boolean isAsc) {
      return isAsc ? v1.compareTo(v2) : v2.compareTo(v1);
    }

    @Override
    public int compare(Pair<List<Object>, List<Object>> o1, Pair<List<Object>, List<Object>> o2) {
      for (int i = 0; i < o1.getLeft().size(); i++) {
        Object v1 = o1.getLeft().get(i);
        Object v2 = o2.getLeft().get(i);
        int res;
        if (v1 instanceof Long) {
          res = compareLong((Long) v1, (Long) v2, isSortedAsc.get(i));
          if (res != 0) {
            return res;
          }
        } else if (v1 instanceof Integer) {
          res = compareInteger((Integer) v1, (Integer) v2, isSortedAsc.get(i));
          if (res != 0) {
            return res;
          }
        } else if (v1 instanceof String) {
          res = compareString((String) v1, (String) v2, isSortedAsc.get(i));
          if (res != 0) {
            return res;
          }
        } else if (v1 instanceof Date) {
          res = compareDate((Date) v1, (Date) v2, isSortedAsc.get(i));
          if (res != 0) {
            return res;
          }
        } else if (v1 instanceof BigDecimal) {
          res = compareBigDecimal((BigDecimal) v1, (BigDecimal) v2, isSortedAsc.get(i));
          if (res != 0) {
            return res;
          }
        } else {
          res = compareDouble((Double) v1, (Double) v2, isSortedAsc.get(i));
          if (res != 0) {
            return res;
          }
        }
      }
      return 0;
    }
  }

}
