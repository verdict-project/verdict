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

package org.verdictdb.core.querying;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class QueryNodeWithPlaceHolders extends QueryNodeBase {

  private static final long serialVersionUID = 5770210201301837177L;
  
  List<PlaceHolderRecord> placeholderRecords = new ArrayList<>();

  // use this to compress the placeholderTable in filter
  List<SubqueryColumn> placeholderTablesinFilter = new ArrayList<>();
  
  private UniquePlaceholderNameCreator placeholderNameCreator =
      new UniquePlaceholderNameCreator(this);

  public QueryNodeWithPlaceHolders(SelectQuery query) {
    super(query);
  }

  public Pair<BaseTable, SubscriptionTicket> createPlaceHolderTable(String aliasName) {
    Pair<String, String> placeholder = placeholderNameCreator.getUniqueStringPair();
    BaseTable table = new BaseTable(placeholder.getLeft(), placeholder.getRight(), aliasName);
    SubscriptionTicket ticket = createSubscriptionTicket();
    int channel = ticket.getChannel().get();
    placeholderRecords.add(new PlaceHolderRecord(table, channel));
    return Pair.of(table, ticket);
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    if (tokens == null) {
      return null;
    }
    if (tokens.size() < placeholderRecords.size()) {
      throw new VerdictDBValueException("Not enough temp tables to plug into placeholder tables.");
    }
    
    for (ExecutionInfoToken token : tokens) {
      // these names will replace the placeholders
      String actualSchemaName = (String) token.getValue("schemaName");
      String actualTableName = (String) token.getValue("tableName");
      Integer channel = (Integer) token.getValue("channel");
      if (actualSchemaName == null || actualTableName == null || channel == null) {
        continue;
      }
  
      for (PlaceHolderRecord record : placeholderRecords) {
        BaseTable placeholderTable = record.getPlaceholderTable();
        int registeredChannel = record.getSubscriptionChannel();
        if (registeredChannel == channel) {
          BaseTable newBaseTable = new BaseTable(actualSchemaName, actualTableName);
          findPlaceHolderAndReplace(placeholderTable, newBaseTable);
        }
      }
    }

//    for (int i = 0; i < placeholderRecords.size(); i++) {
//      PlaceHolderRecord record = placeholderRecords.get(i);
//      BaseTable t = record.getPlaceholderTable();
////      ExecutionInfoToken r = tokens.get(i);
////      String schemaName = (String) r.getValue("schemaName");
////      String tableName = (String) r.getValue("tableName");
////      t.setSchemaName(schemaName);
////      t.setTableName(tableName);
//      //      System.out.println("!!placeholder replacement!!  \n" +
//      //                         new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE) + "\n" +
//      //                         schemaName + " " + tableName);
//    }

    return selectQuery;
  }
  
  /**
   * Examine the selectQuery and replace the schema and table names with the new ones.
   *
   * @param placeholderTable The old table
   * @param actualTable The new table
   */
  private void findPlaceHolderAndReplace(BaseTable placeholderTable, BaseTable actualTable) {
    SelectQuery selectQuery = getSelectQuery();
    
    // check the source list
    for (AbstractRelation source : selectQuery.getFromList()) {
      findPlaceHolderAndReplaceInSource(source, placeholderTable, actualTable);
    }
    
    // check the filter
    if (selectQuery.getFilter().isPresent()) {
      UnnamedColumn filter = selectQuery.getFilter().get();
      findPlaceHolderAndReplaceInFilter(filter, placeholderTable, actualTable);
    }
    
    setSelectQuery(selectQuery);
  }
  
  private void findPlaceHolderAndReplaceInSource(
      AbstractRelation source, BaseTable placeholderTable, BaseTable actualTable) {
    
    if (source instanceof BaseTable) {
      BaseTable baseTableSource = (BaseTable) source;
      if (baseTableSource.equals(placeholderTable)) {
        baseTableSource.setSchemaName(actualTable.getSchemaName());
        baseTableSource.setTableName(actualTable.getTableName());
      }
    } else if (source instanceof JoinTable) {
      for (AbstractRelation joinSource : ((JoinTable) source).getJoinList()) {
        findPlaceHolderAndReplaceInSource(joinSource, placeholderTable, actualTable);
      }
    } else if (source instanceof SelectQuery) {
      SelectQuery subquery = (SelectQuery) source;
  
      // check the source list
      for (AbstractRelation subsource : subquery.getFromList()) {
        findPlaceHolderAndReplaceInSource(subsource, placeholderTable, actualTable);
      }
  
      // check the filter
      if (subquery.getFilter().isPresent()) {
        UnnamedColumn subfilter = subquery.getFilter().get();
        findPlaceHolderAndReplaceInFilter(subfilter, placeholderTable, actualTable);
      }
    } else if (source instanceof SetOperationRelation) {
      SetOperationRelation unionQuery = (SetOperationRelation) source;
      AbstractRelation leftSource = unionQuery.getLeft();
      AbstractRelation rightSource = unionQuery.getRight();
      findPlaceHolderAndReplaceInSource(leftSource, placeholderTable, actualTable);
      findPlaceHolderAndReplaceInSource(rightSource, placeholderTable, actualTable);
    }
  }
  
  private void findPlaceHolderAndReplaceInFilter(
      UnnamedColumn filter, BaseTable placeholderTable, BaseTable actualTable) {
    
    if (filter instanceof ColumnOp) {
      ColumnOp opFilter = (ColumnOp) filter;
      for (UnnamedColumn operand : opFilter.getOperands()) {
        findPlaceHolderAndReplaceInFilter(operand, placeholderTable, actualTable);
      }
    } else if (filter instanceof SubqueryColumn) {
      SelectQuery subquery = ((SubqueryColumn) filter).getSubquery();
      
      // check the source list
      for (AbstractRelation subsource : subquery.getFromList()) {
        findPlaceHolderAndReplaceInSource(subsource, placeholderTable, actualTable);
      }
  
      // check the filter
      if (subquery.getFilter().isPresent()) {
        UnnamedColumn subfilter = subquery.getFilter().get();
        findPlaceHolderAndReplaceInFilter(subfilter, placeholderTable, actualTable);
      }
    }
  }
  
  /**
   * Finds and removes the placeholder that is associated with the given channel
   * @param channel The channel number
   * @return The removed record
   */
  public PlaceHolderRecord removePlaceholderRecordForChannel(int channel) {
    int indexToRemove = 0;
    for (int i = 0; i < placeholderRecords.size(); i++) {
      PlaceHolderRecord record = placeholderRecords.get(i);
      if (record.getSubscriptionChannel() == channel) {
        indexToRemove = i;
        break;
      }
    }
    PlaceHolderRecord removed = placeholderRecords.remove(indexToRemove);
    return removed;
  }
  
  public void addPlaceholderRecord(PlaceHolderRecord record) {
    placeholderRecords.add(record);
  }

  public List<PlaceHolderRecord> getPlaceholderRecords() {
    return placeholderRecords;
  }
  
  public List<BaseTable> getPlaceholderTables() {
    List<BaseTable> tables = new ArrayList<>();
    for (PlaceHolderRecord record : placeholderRecords) {
      tables.add(record.getPlaceholderTable());
    }
    return tables;
  }

  public List<SubqueryColumn> getPlaceholderTablesinFilter() {
    return placeholderTablesinFilter;
  }

  protected void copyFields(QueryNodeWithPlaceHolders from, QueryNodeWithPlaceHolders to) {
    super.copyFields(from, to);
    to.placeholderRecords = new ArrayList<>();
    to.placeholderRecords.addAll(from.placeholderRecords);
    to.placeholderTablesinFilter = new ArrayList<>();
    to.placeholderTablesinFilter.addAll(from.placeholderTablesinFilter);
    deepcopyPlaceHolderTable(to.placeholderRecords, to.selectQuery);
  }

  private void deepcopyPlaceHolderTable(List<PlaceHolderRecord> records, SelectQuery relation) {
    List<BaseTable> to = new ArrayList<>();
    for (PlaceHolderRecord r : records) {
      to.add(r.getPlaceholderTable());
    }
    
    List<SelectQuery> queries = new ArrayList<>();
    queries.add(relation);
    while (!queries.isEmpty()) {
      SelectQuery query = queries.get(0);
      queries.remove(0);
      for (AbstractRelation t : query.getFromList()) {
        if (t instanceof BaseTable && to.contains(t)) {
          BaseTable newT =
              new BaseTable(((BaseTable) t).getSchemaName(), ((BaseTable) t).getTableName());
          if (t.getAliasName().isPresent()) newT.setAliasName(t.getAliasName().get());
          query.getFromList().set(query.getFromList().indexOf(t), newT);
          to.set(to.indexOf(t), newT);
        } else if (t instanceof SelectQuery) queries.add((SelectQuery) t);
        else if (t instanceof JoinTable) {
          for (AbstractRelation join : ((JoinTable) t).getJoinList()) {
            if (join instanceof BaseTable && to.contains(join)) {
              BaseTable newT =
                  new BaseTable(
                      ((BaseTable) join).getSchemaName(), ((BaseTable) join).getTableName());
              if (join.getAliasName().isPresent()) newT.setAliasName(join.getAliasName().get());
              ((JoinTable) t).getJoinList().set(((JoinTable) t).getJoinList().indexOf(join), newT);
              to.set(to.indexOf(join), newT);
            } else if (join instanceof SelectQuery) queries.add((SelectQuery) join);
          }
        }
      }
    }
  }
}

class PlaceHolderRecord implements Serializable {

  private BaseTable placeholderTable;
  
  private int subscriptionChannel;
  
  public PlaceHolderRecord(BaseTable placeholderTable, int subscriptionChannel) {
    this.placeholderTable = placeholderTable;
    this.subscriptionChannel = subscriptionChannel;
  }
  
  public BaseTable getPlaceholderTable() {
    return placeholderTable;
  }
  
  public int getSubscriptionChannel() {
    return subscriptionChannel;
  }
}

class UniquePlaceholderNameCreator implements Serializable {

  private int identifier = 0;
  
  private QueryNodeWithPlaceHolders parent;
  
  public UniquePlaceholderNameCreator(QueryNodeWithPlaceHolders parent) {
    this.parent = parent;
  }
  
  private int getParentCode() {
    return parent.getId();
  }
  
  Pair<String, String> getUniqueStringPair() {
    int parentCode = getParentCode();
    String schemaName = String.format("placeholderSchema_%d_%d", parentCode, identifier);
    String tableName = String.format("placeholderTable_%d_%d", parentCode, identifier);
    identifier += 1;
    return Pair.of(schemaName, tableName);
  }

}