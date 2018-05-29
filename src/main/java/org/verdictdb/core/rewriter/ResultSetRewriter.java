package org.verdictdb.core.rewriter;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.exception.ValueException;
import org.verdictdb.exception.VerdictDbException;

import tech.tablesaw.api.Table;

public class ResultSetRewriter {

  /**
   * Rewrites the raw result set into something meaningful.
   * 
   * Rewritten result set should have the following columns:
   * 1. groups (that appear in the group by clause)
   * 2. aggregate columns
   * 3. 
   * 
   * @param rawResultSet
   * @param aggColumns Each pair is (original agg alias, type of agg)
   * @return
   * @throws VerdictDbException 
   */
  public Table rewrite(Table rawResultSet, List<Pair<String, String>> aggColumns) throws VerdictDbException {
    List<String> columnNames = rawResultSet.columnNames();
    
    // validity check
    for (Pair<String, String> agg : aggColumns) {
      String aggAlias = agg.getLeft();
      String aggType = agg.getRight();
      String aliasName = RewritingRules.aggAliasName(aggAlias, aggType);
      if (!columnNames.contains(aliasName)) {
        throw new ValueException("The expected column name does not exist: " + aliasName);
      }
    }
    
    return null;
  }
  
}
