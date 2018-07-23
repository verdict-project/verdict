/*
 *    Copyright 2017 University of Michigan
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

package org.verdictdb.core.rewriter.aggresult;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.aggresult.AggregateFrame;
import org.verdictdb.core.rewriter.query.AggblockMeta;

public class AggResultCombiner {

  public static Pair<AggregateFrame, AggblockMeta> combine(
      AggregateFrame agg1,
      AggblockMeta aggblockMeta1,
      AggregateFrame agg2,
      AggblockMeta aggblockMeta2) {
    return Pair.of(agg2, aggblockMeta2);
  }

  public static Pair<AggregateFrame, AggblockMeta> scaleSingle(
      AggregateFrame left, AggblockMeta right) {
    // TODO Auto-generated method stub
    return null;
  }
}
