package org.verdictdb.core.scrambling;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.sqlobject.SelectQuery;

public interface StatiticsQueryGenerator {
  
  public SelectQuery create(
      String schemaName, 
      String tableName, 
      List<Pair<String, String>> columnNamesAndTypes, 
      List<String> partitionColumnNames);

}
