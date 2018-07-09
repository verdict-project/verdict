package org.verdictdb.core.sqlobject;

public class SetOperationRelation extends AbstractRelation  {

  //May need to expand
  public enum SetOpType {
    union, unionAll, except, intersect
  }

  AbstractRelation left, right;

  SetOpType setOpType;

  public SetOperationRelation (AbstractRelation left, AbstractRelation right, SetOpType setOpType) {
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
    }
    else if (setOpType.equals(SetOpType.unionAll)) {
      return "UNION ALL";
    }
    else if (setOpType.equals(SetOpType.except)) {
      return "EXCEPT";
    }
    else if (setOpType.equals(SetOpType.intersect)) {
      return "INTERSECT";
    }
    else return "UNION";
  }
}
