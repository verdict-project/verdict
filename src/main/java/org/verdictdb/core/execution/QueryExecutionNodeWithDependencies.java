package org.verdictdb.core.execution;

import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.JoinTable;
import org.verdictdb.core.query.SelectQuery;

public class QueryExecutionNodeWithDependencies extends CreateTableAsSelectTrait {

  protected QueryExecutionNodeWithDependencies(String scratchpadSchemaName) {
    super(scratchpadSchemaName);
  }

  static void convertSubqueriesIntoDependentNodes (
      SelectQuery query, 
      CreateTableAsSelectTrait node) {
    
    // from list
    for (AbstractRelation source : query.getFromList()) {
      int index = query.getFromList().indexOf(source);
      
      // If the table is subquery, we need to add it to dependency
      if (source instanceof SelectQuery) {
        if (source.isAggregateQuery()) {
          AggExecutionNode dep = AggExecutionNode.create((SelectQuery) source, node.scratchpadSchemaName);
          node.addDependency(dep);
        } else {
          ProjectionExecutionNode dep = ProjectionExecutionNode.create((SelectQuery) source, node.scratchpadSchemaName);
          node.addDependency(dep);
        }
        
        // use placeholders to mark the locations whose names will be updated in the future
        BaseTable base = node.createPlaceHolderTable(source.getAliasName().get());
        query.getFromList().set(index, base);
        
//        if (source.getAliasName().isPresent()) {
//          
//        } else 
//          query.getFromList().set(index, new BaseTable(schemaName, temptableName, temptableName));
      } 
      else if (source instanceof JoinTable) {
        for (AbstractRelation s : ((JoinTable) source).getJoinList()) {
          int joinindex = ((JoinTable) source).getJoinList().indexOf(s);
          
          // If the table is subquery, we need to add it to dependency
          if (s instanceof SelectQuery) {
            if (s.isAggregateQuery()) {
              AggExecutionNode dep = AggExecutionNode.create((SelectQuery) s, node.scratchpadSchemaName);
              node.addDependency(dep);
            } else {
              ProjectionExecutionNode dep = ProjectionExecutionNode.create((SelectQuery) s, node.scratchpadSchemaName);
              node.addDependency(dep);
            }
            
            // use placeholders to mark the locations whose names will be updated in the future
            BaseTable base = node.createPlaceHolderTable(s.getAliasName().get());
            ((JoinTable) source).getJoinList().set(joinindex, base);
            
//            if (source.getAliasName().isPresent()) {
//              ((JoinTable) source).getJoinList().set(joinindex, new BaseTable(schemaName, temptableName, source.getAliasName().get()));
//            } else ((JoinTable) source).getJoinList().set(joinindex, new BaseTable(schemaName, temptableName, temptableName));
          }
        }
      }
    }

//    // Filter
//    if (query.getFilter().isPresent()) {
//      UnnamedColumn where = query.getFilter().get();
//      List<UnnamedColumn> filters = new ArrayList<>();
//      filters.add(where);
//      while (!filters.isEmpty()) {
//        UnnamedColumn filter = filters.get(0);
//        filters.remove(0);
//        // If filter is a subquery, we need to add it to dependency
//        if (filter instanceof SubqueryColumn) {
//          String temptableName = QueryExecutionPlan.generateTempTableName();
//          if (((SubqueryColumn) filter).getSubquery().isAggregateQuery()) {
//            addDependency(new AggExecutionNode(conn, schemaName, temptableName, ((SubqueryColumn) filter).getSubquery()));
//          } else {
//            addDependency(new ProjectionExecutionNode(conn, schemaName, temptableName, ((SubqueryColumn) filter).getSubquery()));
//          }
//          // To replace the subquery, we use the selectlist of the subquery and tempTable to create a new non-aggregate subquery
//          List<SelectItem> newSelectItem = new ArrayList<>();
//          for (SelectItem item:((SubqueryColumn) filter).getSubquery().getSelectList()) {
//            if (item instanceof AliasedColumn) {
//              newSelectItem.add(new AliasedColumn(new BaseColumn(schemaName, temptableName,
//                  ((AliasedColumn) item).getAliasName()), ((AliasedColumn) item).getAliasName()));
//            } else if (item instanceof AsteriskColumn) {
//              newSelectItem.add(new AsteriskColumn());
//            } else throw new VerdictDbException("Select list contains SelectItem type that is not AliasedColumn or AsteriskColumn");
//          }
//          SelectQuery newSubquery = SelectQuery.create(newSelectItem, new BaseTable(schemaName, temptableName, temptableName));
//          if (((SubqueryColumn) filter).getSubquery().getAliasName().isPresent()) {
//            newSubquery.setAliasName(((SubqueryColumn) filter).getSubquery().getAliasName().get());
//          }
//          ((SubqueryColumn) filter).setSubquery(newSubquery);
//        } else if (filter instanceof ColumnOp) {
//          filters.addAll(((ColumnOp) filter).getOperands());
//        }
//      }
//    }
  }
  
}
