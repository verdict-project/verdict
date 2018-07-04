package org.verdictdb.core.querying;

import org.apache.commons.lang3.tuple.Pair;

public interface IdCreator {
  
  public String generateAliasName();
  
  public Pair<String, String> generateTempTableName();
  
}
