package org.verdictdb.core.querying;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.tuple.Pair;

public class TempIdCreatorInScratchpadSchema implements IdCreator {
  
  String scratchpadSchemaName;

  final int serialNum = ThreadLocalRandom.current().nextInt(0, 1000000);

  int identifierNum = 0;
  
  public TempIdCreatorInScratchpadSchema(String scratchpadSchemaName) {
    this.scratchpadSchemaName = scratchpadSchemaName;
  }
  
  public int getSerialNumber() {
    return serialNum;
  }
  
  public void reset() {
    identifierNum = 0;
  }
  
  public String getScratchpadSchemaName() {
    return scratchpadSchemaName;
  }
  
  synchronized String generateUniqueIdentifier() {
    return String.format("%d_%d", serialNum, identifierNum++);
  }

  public String generateAliasName() {
    return String.format("verdictdbalias_%s", generateUniqueIdentifier());
  }

  public Pair<String, String> generateTempTableName() {
    //    return Pair.of(scratchpadSchemaName, String.format("verdictdbtemptable_%d", tempTableNameNum++));
    return Pair.of(scratchpadSchemaName, String.format("verdictdbtemptable_%s", generateUniqueIdentifier()));
  }


}
