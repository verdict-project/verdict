package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.core.query.SelectQueryOp;

/**
 * Will be used to handle nested queries (i.e., the queries including aggregate queries as subqueries).
 * 
 * @author Yongjoo Park
 *
 */
public class AggExecutionNode {
  
  List<AggColumn> aggColumns = new ArrayList<>();
  
  SelectQueryOp originalQuery;
  
  SelectQueryOp rewrittenQuery;

}

class AggColumn {
  
  String aliasName;
  
  String aggType;   // one of "sum", "count", or "avg"
  
  public static AggColumn of(String aliasName, String aggType) {
    AggColumn a = new AggColumn();
    a.aliasName = aliasName;
    a.aggType = aggType;
    return a;
  }
  
}
