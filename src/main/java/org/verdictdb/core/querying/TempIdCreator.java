package org.verdictdb.core.querying;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.tuple.Pair;

public interface TempIdCreator {
  
  public String generateAliasName();
  
  public Pair<String, String> generateTempTableName();
  
}
