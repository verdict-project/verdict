package org.verdictdb.core.querying.simplifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.CreateTableAsSelectNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.PlaceHolderRecord;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.querying.QueryNodeWithPlaceHolders;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;

import com.google.common.base.Optional;

public class DirectRetrievalExecutionNode extends QueryNodeWithPlaceHolders {

  private static final long serialVersionUID = -561220173745897906L;
  
  private QueryNodeWithPlaceHolders parentNode;
  
  private CreateTableAsSelectNode childNode;

  public DirectRetrievalExecutionNode(
      SelectQuery selectQuery,
      QueryNodeWithPlaceHolders parent,
      CreateTableAsSelectNode child) {
    super(parent.getId(), selectQuery);
    parentNode = parent;
    childNode = child;
    
//    for (PlaceHolderRecord record : parentNode.getPlaceholderRecords()) {
//      addPlaceholderRecord(record);
//    }
//    for (PlaceHolderRecord record : childNode.getPlaceholderRecords()) {
//      addPlaceholderRecord(record);
//    }
  }
  
  @Override
  public PlaceHolderRecord removePlaceholderRecordForChannel(int channel) {
    PlaceHolderRecord record = parentNode.removePlaceholderRecordForChannel(channel);
    if (record != null) {
      return record;
    }
    record = childNode.removePlaceholderRecordForChannel(channel);
    return record;
  }
  
  public static DirectRetrievalExecutionNode create(
      QueryNodeWithPlaceHolders parent,
      CreateTableAsSelectNode child) {
    
    SelectQuery parentQuery = parent.getSelectQuery();
    
    // find place holders in the parent query and replace with the child query
    int childChannel = parent.getChannelForSource(child);
    PlaceHolderRecord placeHolderToRemove = parent.removePlaceholderRecordForChannel(childChannel);
    BaseTable baseTableToRemove = placeHolderToRemove.getPlaceholderTable();
    
    // replace in the source list
    parentQuery = (SelectQuery) consolidateSource(parentQuery, child, baseTableToRemove);
//    List<AbstractRelation> parentFromList = parentQuery.getFromList();
//    List<AbstractRelation> newParentFromList = new ArrayList<>();
//    for (AbstractRelation originalSource : parentFromList) {
//      AbstractRelation newSource = consolidateSource(originalSource, child, baseTableToRemove);
//      newParentFromList.add(newSource);
//    }
//    parentQuery.setFromList(newParentFromList);
    
    // Filter: replace the placeholder BaseTable (in the filter list) with
    // the SelectQuery of the child
    Optional<UnnamedColumn> parentFilterOptional = parentQuery.getFilter();
    if (parentFilterOptional.isPresent()) {
      UnnamedColumn originalFilter = parentFilterOptional.get();
      UnnamedColumn newFilter = consolidateFilter(originalFilter, child, baseTableToRemove);
      parentQuery.clearFilter();
      parentQuery.addFilterByAnd(newFilter);
    }
    
    DirectRetrievalExecutionNode node = 
        new DirectRetrievalExecutionNode(parentQuery, parent, child);
    return node;
  }
  
  /**
   * May consonlidate a single source with `child`. This is a helper function for simplify2().
   *
   * @param originalSource The original source
   * @param child The child node
   * @param baseTableToRemove The placeholder to be replaced
   * @return A new source
   */
  private static AbstractRelation consolidateSource(
      AbstractRelation originalSource, ExecutableNodeBase child, BaseTable baseTableToRemove) {
  
    // exception
    if (!(child instanceof QueryNodeBase)) {
      return originalSource;
    }
    
    SelectQuery childQuery = ((QueryNodeBase) child).getSelectQuery();
    
    if (originalSource instanceof BaseTable) {
      BaseTable baseTableSource = (BaseTable) originalSource;
      if (baseTableSource.equals(baseTableToRemove)) {
        childQuery.setAliasName(baseTableToRemove.getAliasName().get());
        return childQuery;
      } else {
        return originalSource;
      }
    } else if (originalSource instanceof JoinTable) {
      JoinTable joinTableSource = (JoinTable) originalSource;
      List<AbstractRelation> joinSourceList = joinTableSource.getJoinList();
      List<AbstractRelation> newJoinSourceList = new ArrayList<>();
      for (AbstractRelation joinSource : joinSourceList) {
        newJoinSourceList.add(consolidateSource(joinSource, child, baseTableToRemove));
      }
      joinTableSource.setJoinList(newJoinSourceList);
      return joinTableSource;
      
    } else if (originalSource instanceof SelectQuery) {
      SelectQuery selectQuerySource = (SelectQuery) originalSource;
      List<AbstractRelation> originalFromList = selectQuerySource.getFromList();
      List<AbstractRelation> newFromList = new ArrayList<>();
      for (AbstractRelation source : originalFromList) {
        AbstractRelation newSource = consolidateSource(source, child, baseTableToRemove);
        newFromList.add(newSource);
      }
      selectQuerySource.setFromList(newFromList);
      return selectQuerySource;
      
    } else {
      return originalSource;
    }
  }
  
  /**
   * May consolidate a single filter with `child`. This is a helper function for simplify2().
   *
   * @param originalFilter The original filter
   * @param child The child node
   * @param baseTableToRemove The placeholder to be replaced
   * @return
   */
  private static UnnamedColumn consolidateFilter(
      UnnamedColumn originalFilter, ExecutableNodeBase child, BaseTable baseTableToRemove) {
    
    // exception
    if (!(child instanceof QueryNodeBase)) {
      return originalFilter;
    }
    
    SelectQuery childSelectQuery = ((QueryNodeBase) child).getSelectQuery();
  
    if (originalFilter instanceof ColumnOp) {
      ColumnOp originalColumnOp = (ColumnOp) originalFilter;
      List<UnnamedColumn> newOperands = new ArrayList<>();
      for (UnnamedColumn o : originalColumnOp.getOperands()) {
        newOperands.add(consolidateFilter(o, child, baseTableToRemove));
      }
      ColumnOp newColumnOp = new ColumnOp(originalColumnOp.getOpType(), newOperands);
      return newColumnOp;
    } else if (originalFilter instanceof SubqueryColumn) {
      SubqueryColumn originalSubquery = (SubqueryColumn) originalFilter;
      SelectQuery subquery = originalSubquery.getSubquery();
      List<AbstractRelation> subqueryFromList = subquery.getFromList();
      if (subqueryFromList.size() == 1 && subqueryFromList.get(0).equals(baseTableToRemove)) {
        originalSubquery.setSubquery(childSelectQuery);
      }
      return originalSubquery;
    }
    
    return originalFilter;
  }
  
  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    // this will replace the placeholders contained the subquery.
    childNode.createQuery(tokens);
    ExecutionInfoToken childToken = childNode.createToken(null);
    
    // also pass the tokens possibly created by child.
    List<ExecutionInfoToken> newTokens = new ArrayList<>();
    newTokens.addAll(tokens);
    newTokens.add(childToken);
    parentNode.createQuery(newTokens);
    return selectQuery;
  }
  
  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("queryResult", result);
    return token;
  }

}
