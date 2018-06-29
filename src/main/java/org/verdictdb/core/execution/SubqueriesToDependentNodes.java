package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.AsteriskColumn;
import org.verdictdb.core.query.BaseColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.JoinTable;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.query.SubqueryColumn;
import org.verdictdb.core.query.UnnamedColumn;

public class SubqueriesToDependentNodes {

  /**
   * 
   * @param query A query that may include subqueries. The subqueries of this query will be replaced by
   * placeholders.
   * @param node
   */
  public static void convertSubqueriesToDependentNodes(
      SelectQuery query, 
      CreateTableAsSelectExecutionNode node) {
    QueryExecutionPlan plan = node.getPlan();
    
    // from list
    for (AbstractRelation source : query.getFromList()) {
      int index = query.getFromList().indexOf(source);

      // If the table is subquery, we need to add it to dependency
      if (source instanceof SelectQuery) {
        CreateTableAsSelectExecutionNode dep;
        if (source.isAggregateQuery()) {
          dep = AggExecutionNode.create(plan, (SelectQuery) source);
        } else {
          dep = ProjectionExecutionNode.create(plan, (SelectQuery) source);
        }
        node.addDependency(dep);

        // use placeholders to mark the locations whose names will be updated in the future
        Pair<BaseTable, ExecutionTokenQueue> baseAndQueue = node.createPlaceHolderTable(source.getAliasName().get());
        query.getFromList().set(index, baseAndQueue.getLeft());
        dep.addBroadcastingQueue(baseAndQueue.getRight());
      } 
      else if (source instanceof JoinTable) {
        for (AbstractRelation s : ((JoinTable) source).getJoinList()) {
          int joinindex = ((JoinTable) source).getJoinList().indexOf(s);

          // If the table is subquery, we need to add it to dependency
          if (s instanceof SelectQuery) {
            CreateTableAsSelectExecutionNode dep;
            if (s.isAggregateQuery()) {
              dep = AggExecutionNode.create(plan, (SelectQuery) s);
            } else {
              dep = ProjectionExecutionNode.create(plan, (SelectQuery) s);
            }
            node.addDependency(dep);

            // use placeholders to mark the locations whose names will be updated in the future
            Pair<BaseTable, ExecutionTokenQueue> baseAndQueue = node.createPlaceHolderTable(s.getAliasName().get());
            ((JoinTable) source).getJoinList().set(joinindex, baseAndQueue.getLeft());
            dep.addBroadcastingQueue(baseAndQueue.getRight());
          }
        }
      }
    }

//    int filterPlaceholderNum = 0;
    // Filter
    if (query.getFilter().isPresent()) {
      UnnamedColumn where = query.getFilter().get();
      List<UnnamedColumn> filters = new ArrayList<>();
      filters.add(where);
      while (!filters.isEmpty()) {
        UnnamedColumn filter = filters.get(0);
        filters.remove(0);
        
        // If filter is a subquery, we need to add it to dependency
        if (filter instanceof SubqueryColumn) {
          Pair<BaseTable, ExecutionTokenQueue> baseAndQueue;
          if (((SubqueryColumn) filter).getSubquery().getAliasName().isPresent()){
            baseAndQueue = node.createPlaceHolderTable(((SubqueryColumn) filter).getSubquery().getAliasName().get());
          } else {
//            baseAndQueue = node.createPlaceHolderTable("filterPlaceholder"+filterPlaceholderNum++);
            baseAndQueue = node.createPlaceHolderTable(plan.generateAliasName());
          }
          BaseTable base = baseAndQueue.getLeft();

          CreateTableAsSelectExecutionNode dep;
          SelectQuery subquery = ((SubqueryColumn) filter).getSubquery();
          if (subquery.isAggregateQuery()) {
            dep = AggExecutionNode.create(plan, subquery);
            node.addDependency(dep);
          } else {
            dep = ProjectionExecutionNode.create(plan, subquery);
            node.addDependency(dep);
          }
          dep.addBroadcastingQueue(baseAndQueue.getRight());
          
          // To replace the subquery, we use the selectlist of the subquery and tempTable to create a new non-aggregate subquery
          List<SelectItem> newSelectItem = new ArrayList<>();
          for (SelectItem item : subquery.getSelectList()) {
            if (item instanceof AliasedColumn) {
              newSelectItem.add(new AliasedColumn(
                  new BaseColumn(
                      base.getSchemaName(), base.getAliasName().get(), 
                      ((AliasedColumn) item).getAliasName()), 
                  ((AliasedColumn) item).getAliasName()));
            } else if (item instanceof AsteriskColumn) {
              newSelectItem.add(new AsteriskColumn());
            }
          }
          SelectQuery newSubquery = SelectQuery.create(newSelectItem, base);
          if (((SubqueryColumn) filter).getSubquery().getAliasName().isPresent()) {
            newSubquery.setAliasName(((SubqueryColumn) filter).getSubquery().getAliasName().get());
          }
          ((SubqueryColumn) filter).setSubquery(newSubquery);
        }
        else if (filter instanceof ColumnOp) {
          filters.addAll(((ColumnOp) filter).getOperands());
        }
      }
    }
    
  }
}

