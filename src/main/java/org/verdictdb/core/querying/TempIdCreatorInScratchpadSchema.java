/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.core.querying;

import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;

public class TempIdCreatorInScratchpadSchema implements IdCreator, Serializable {

  private static final long serialVersionUID = -8241890224536966759L;

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

  @Override
  public String generateAliasName() {
    return String.format("verdictdb_alias_%s", generateUniqueIdentifier());
  }
  
  @Override
  public String generateAliasName(String keyword) {
    return String.format("verdictdb_%s_alias_%s", keyword, generateUniqueIdentifier());
  }
  
  @Override
  public Pair<String, String> generateTempTableName() {
    //    return Pair.of(scratchpadSchemaName, String.format("verdictdbtemptable_%d",
    // tempTableNameNum++));
    return Pair.of(
        scratchpadSchemaName, String.format("verdictdbtemptable_%s", generateUniqueIdentifier()));
  }
}
