package org.verdictdb.connection;

import com.google.common.collect.TreeMultiset;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.verdictdb.commons.AttributeValueRetrievalHelper;
import org.verdictdb.core.querying.ola.SelectAsyncAggExecutionNode;
import org.verdictdb.core.sqlobject.*;

import java.math.BigDecimal;
import java.util.*;


public class HashDbmsQueryResult extends AttributeValueRetrievalHelper implements DbmsQueryResult {

  private static final long serialVersionUID = 2576550919489091L;

  // key is the non-aggregate column, value is also HashMap,
  // where the key is the tier value and the value is aggregate columns.
  private HashMap<List<Object>, HashMap<List<Integer>, List<Double>>> results = new HashMap<>();

  private TreeMultiset<List<Object>> rows;

  private List<Object> currentRow;

  private Iterator<List<Object>> rowIterator;

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

  public HashDbmsQueryResult(SelectAsyncAggExecutionNode node) {
    if (node.getSelectQuery().getLimit().isPresent()) {
      limit = (Integer) ((ConstantColumn) node.getSelectQuery().getLimit().get()).getValue();
    }
  }

  public void setIsAggregate(List<Integer> isAggregate) {
    this.isAggregate = isAggregate;
  }

  public void setTierInfo(List<Integer> tierColumnIdx) {
    this.tierColumnIndex = tierColumnIdx;
  }

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
        } else if (!tierColumnIndex.contains(i)) {
          nonAggColumnName.add(result.getColumnName(i));
        }
      }
    }

    // update rows
    List<Integer> sortedColumnIndex = new ArrayList<>();
    List<Boolean> isSortedAsc = new ArrayList<>();
    for (int i = 0; i < result.getColumnCount(); i++) {
      if (result.getColumnName(i).matches("verdictdb_order_by[0-9]+$")) {
        if (node.getSelectQuery().getOrderby().get(sortedColumnIndex.size()).getOrder().equals("asc")) {
          isSortedAsc.add(true);
        } else {
          isSortedAsc.add(false);
        }
        sortedColumnIndex.add(i);
      }
    }
    rows = TreeMultiset.create(new HashMapComparator(sortedColumnIndex, isSortedAsc));

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
        } else if (!tierColumnIndex.contains(i)) {
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
      for (int i = 0; i < node.getSelectQuery().getSelectList().size(); i++) {
        AliasedColumn sel = (AliasedColumn) node.getSelectQuery().getSelectList().get(i);
        if (nonAggColumnName.contains(sel.getAliasName())) {
          // non aggregate
          row.add(entry.getKey().get(nonAggColumnName.indexOf(sel.getAliasName())));
        } else {
          // aggregate
          row.add(calculateAggregateValue(sel.getColumn(), entry));
        }
      }
      rows.add(row);
    }
    rowIterator = rows.iterator();

    // set up meta data
    if (columnName==null) {
      columnName = new ArrayList<>();
      for (SelectItem sel:node.getSelectQuery().getSelectList()) {
        if (sel instanceof AliasedColumn) {
          columnName.add(((AliasedColumn) sel).getAliasName());
        } else {
          columnName.add(null);
        }
      }
    }
  }

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
      currentRow = rowIterator.next();
      cursor++;
      if (limit != null && cursor > limit) {
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


  class HashMapComparator implements Comparator<List<Object>> {

    List<Integer> sortedColumnIndex;

    List<Boolean> isSortedAsc;

    public HashMapComparator(List<Integer> sortedColumnIndex, List<Boolean> isSortedAsc) {
      this.sortedColumnIndex = sortedColumnIndex;
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

    @Override
    public int compare(List<Object> o1, List<Object> o2) {
      for (int i = 0; i < sortedColumnIndex.size(); i++) {
        Object v1 = o1.get(i);
        Object v2 = o2.get(i);
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
