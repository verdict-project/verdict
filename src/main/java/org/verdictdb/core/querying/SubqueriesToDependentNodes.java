package org.verdictdb.core.querying;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBValueException;

public class SubqueriesToDependentNodes {

  /**
   * 
   * @param query A query that may include subqueries. The subqueries of this query will be replaced by
   * placeholders.
   * @param node
   * @throws VerdictDBValueException 
   */
  public static void convertSubqueriesToDependentNodes(
      SelectQuery query, 
      CreateTableAsSelectNode node) throws VerdictDBValueException {
    TempIdCreator namer = node.getNamer();
    
    // from list
    for (AbstractRelation source : query.getFromList()) {
      int index = query.getFromList().indexOf(source);

      // If the table is subquery, we need to add it to dependency
      if (source instanceof SelectQuery) {
        CreateTableAsSelectNode dep;
        if (source.isAggregateQuery()) {
          dep = AggExecutionNode.create(namer, (SelectQuery) source);
        } else {
          dep = ProjectionNode.create(namer, (SelectQuery) source);
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
            CreateTableAsSelectNode dep;
            if (s.isAggregateQuery()) {
              dep = AggExecutionNode.create(namer, (SelectQuery) s);
            } else {
              dep = ProjectionNode.create(namer, (SelectQuery) s);
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
            baseAndQueue = node.createPlaceHolderTable(namer.generateAliasName());
          }
          BaseTable base = baseAndQueue.getLeft();

          CreateTableAsSelectNode dep;
          SelectQuery subquery = ((SubqueryColumn) filter).getSubquery();
          if (subquery.isAggregateQuery()) {
            dep = AggExecutionNode.create(namer, subquery);
            node.addDependency(dep);
          } else {
            dep = ProjectionNode.create(namer, subquery);
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
          node.getPlaceholderTablesinFilter().add((SubqueryColumn) filter);
        }
        else if (filter instanceof ColumnOp) {
          filters.addAll(((ColumnOp) filter).getOperands());
        }
      }
    }
    
  }
}

