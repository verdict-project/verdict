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

package org.verdictdb.core.rewriter.query;

import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This class effectively has the following information. [ { "tier": number, "total": } ... ]
 *
 * @author Yongjoo Park
 */
public class AggblockMeta implements Iterable<Entry<Pair<String, String>, Pair<Integer, Integer>>> {

  Map<Pair<String, String>, Pair<Integer, Integer>> data = new HashMap<>();

  public static AggblockMeta empty() {
    return new AggblockMeta();
  }

  /**
   * @param schemaName
   * @param tableName
   * @param blockSpan Inclusive range on both ends
   */
  public void addMeta(String schemaName, String tableName, Pair<Integer, Integer> blockSpan) {
    data.put(Pair.of(schemaName, tableName), blockSpan);
  }

  @Override
  public Iterator<Entry<Pair<String, String>, Pair<Integer, Integer>>> iterator() {
    return data.entrySet().iterator();
  }
}
