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

package org.verdictdb.core.sqlobject;

public class SetOperationRelation extends AbstractRelation {

  private static final long serialVersionUID = -1691931967730375434L;

  // May need to expand
  public enum SetOpType {
    union,
    unionAll,
    except,
    intersect
  }

  AbstractRelation left, right;

  SetOpType setOpType;

  public SetOperationRelation(AbstractRelation left, AbstractRelation right, SetOpType setOpType) {
    this.left = left;
    this.right = right;
    this.setOpType = setOpType;
  }

  public AbstractRelation getLeft() {
    return left;
  }

  public AbstractRelation getRight() {
    return right;
  }

  public String getSetOpType() {
    if (setOpType.equals(SetOpType.union)) {
      return "UNION";
    } else if (setOpType.equals(SetOpType.unionAll)) {
      return "UNION ALL";
    } else if (setOpType.equals(SetOpType.except)) {
      return "EXCEPT";
    } else if (setOpType.equals(SetOpType.intersect)) {
      return "INTERSECT";
    } else return "UNION";
  }
}
