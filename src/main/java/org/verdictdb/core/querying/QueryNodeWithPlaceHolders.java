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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

public abstract class QueryNodeWithPlaceHolders extends QueryNodeBase {

  private static final long serialVersionUID = 5770210201301837177L;

  List<BaseTable> placeholderTables = new ArrayList<>();

  // use this to compress the placeholderTable in filter
  List<SubqueryColumn> placeholderTablesinFilter = new ArrayList<>();

  public QueryNodeWithPlaceHolders(SelectQuery query) {
    super(query);
  }

  public Pair<BaseTable, SubscriptionTicket> createPlaceHolderTable(String aliasName) {
    //      throws VerdictDBValueException {
    BaseTable table = new BaseTable("placeholderSchemaName", "placeholderTableName", aliasName);
    placeholderTables.add(table);
    //    ExecutionTokenQueue listeningQueue = generateListeningQueue();
    SubscriptionTicket ticket = createSubscriptionTicket();
    return Pair.of(table, ticket);
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    if (tokens == null) {
      return null;
    }
    if (tokens.size() < placeholderTables.size()) {
      throw new VerdictDBValueException("Not enough temp tables to plug into placeholder tables.");
    }

    for (int i = 0; i < placeholderTables.size(); i++) {
      BaseTable t = placeholderTables.get(i);
      ExecutionInfoToken r = tokens.get(i);
      String schemaName = (String) r.getValue("schemaName");
      String tableName = (String) r.getValue("tableName");
      t.setSchemaName(schemaName);
      t.setTableName(tableName);
      //      System.out.println("!!placeholder replacement!!  \n" +
      //                         new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE) + "\n" +
      //                         schemaName + " " + tableName);
    }

    return selectQuery;
  }

  //  @Override
  //  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken>
  // downstreamResults)
  //      throws VerdictDBException {
  //    if (downstreamResults==null) { return null; }
  //    if (downstreamResults.size() < placeholderTables.size()) {
  //      throw new VerdictDBValueException("Not enough temp tables to plug into placeholder
  // tables.");
  //    }
  //
  //    for (int i = 0; i < placeholderTables.size(); i++) {
  //      BaseTable t = placeholderTables.get(i);
  //      ExecutionInfoToken r = downstreamResults.get(i);
  //      String schemaName = (String) r.getValue("schemaName");
  //      String tableName = (String) r.getValue("tableName");
  //      t.setSchemaName(schemaName);
  //      t.setTableName(tableName);
  //    }
  //    return null;
  //  }

  public List<BaseTable> getPlaceholderTables() {
    return placeholderTables;
  }

  public List<SubqueryColumn> getPlaceholderTablesinFilter() {
    return placeholderTablesinFilter;
  }

  protected void copyFields(QueryNodeWithPlaceHolders from, QueryNodeWithPlaceHolders to) {
    super.copyFields(from, to);
    to.placeholderTables = new ArrayList<>();
    to.placeholderTables.addAll(from.placeholderTables);
    to.placeholderTablesinFilter = new ArrayList<>();
    to.placeholderTablesinFilter.addAll(from.placeholderTablesinFilter);
    deepcopyPlaceHolderTable(to.placeholderTables, to.selectQuery);
  }

  private void deepcopyPlaceHolderTable(List<BaseTable> to, SelectQuery relation) {
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
