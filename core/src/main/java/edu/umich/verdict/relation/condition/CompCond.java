/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.relation.condition;

import java.util.List;

import edu.umich.verdict.relation.*;
import edu.umich.verdict.relation.expr.*;
import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import parquet.io.api.Binary;

public class CompCond extends Cond {

    private Expr left;

    private Expr right;

    private String compOp;

    public CompCond(Expr left, String compOp, Expr right) {
        this.left = left;
        this.right = right;
        this.compOp = compOp;
    }

    public static CompCond from(VerdictContext vc, Expr left, String compOp, Expr right) {
        return new CompCond(left, compOp, right);
    }

    public static CompCond from(VerdictContext vc, Expr left, String compOp, Relation r) {
        return from(vc, left, compOp, SubqueryExpr.from(vc, r));
    }

    public static CompCond from(VerdictContext vc, String left, String compOp, Relation r) {
        return from(vc, Expr.from(vc, left), compOp, SubqueryExpr.from(vc, r));
    }

    public Expr getLeft() {
        return left;
    }

    public Expr getRight() {
        return right;
    }

    public String getOp() {
        return compOp;
    }

    @Override
    public Cond accept(CondModifier v) {
        return v.call(this);
    }

    @Override
    public String toString() {
        return String.format("%s %s %s", left, compOp, right);
    }

    @Override
    public Pair<Cond, Pair<ExactRelation, ExactRelation>> searchForJoinCondition(List<ExactRelation> tableSources) {
        if (compOp.equals("=")) {
            if (left instanceof ColNameExpr && right instanceof ColNameExpr) {
                String leftTab = ((ColNameExpr) left).getTab();
                String rightTab = ((ColNameExpr) right).getTab();
                ExactRelation r1 = tableSources.get(0);
                ExactRelation r2 = null;

                if (doesRelationContain(r1, leftTab)) {
                    r2 = findSourceContaining(tableSources, rightTab);
                    if (r2 != null && r2 instanceof JoinedRelation) {
                        r2 = findSingleRelation(r2, rightTab);
                    }
                } else if (doesRelationContain(r1, rightTab)) {
                    r2 = findSourceContaining(tableSources, leftTab);
                    if (r2 != null && r2 instanceof JoinedRelation) {
                        r2 = findSingleRelation(r2, leftTab);
                    }
                }

                String leftOriginalName = getOriginalTableName(tableSources, leftTab);
                String rightOriginalName = getOriginalTableName(tableSources, rightTab);
                if (r2 != null && leftOriginalName != null && rightOriginalName != null &&
                        !leftOriginalName.equals(rightOriginalName)) {
                    return Pair.of((Cond) this, Pair.of(r1, r2));
                }

                // if (!joinedTableAliases.contains(leftTab) &&
                // !joinedTableAliases.contains(rightTab)) {
                // // the condition does not contain any pre-joined tables.
                // if (leftTab != rightTab) {
                // return Triple.of((Cond) this, Pair.of(leftTab, rightTab), true);
                // }
                // } else {
                // // if there are some tables that are already joined, a new table must be a
                // new table.
                // if (joinedTableAliases.contains(leftTab) &&
                // !joinedTableAliases.contains(rightTab)) {
                // return Triple.of((Cond) this, Pair.of(leftTab, rightTab));
                // } else if (!joinedTableAliases.contains(leftTab) &&
                // joinedTableAliases.contains(rightTab)) {
                // return Triple.of((Cond) this, Pair.of(rightTab, leftTab));
                // }
                // }
            }
        }
        return null;
    }

    // dyoon: currently, this function actually replace searchForJoinCondition.
    // After we ensure that searchForJoinCondition is no longer needed. We should
    // replace it with this function.
    /**
     * Extracts join relations if equal operator was used for an inner join.
     * it extracts join relations if equal operator has either ColNameExpr or BinaryOpExpr
     * on either side. For BinaryOpExpr, it can only parse simple expressions in the format of
     * <column> <op> <constant>
     * @param tableSources
     * @return a pair of Cond operator and a pair of left and right join tables.
     * returns null if it is not an inner join.
     */
    @Override
    public Pair<Cond, Pair<ExactRelation, ExactRelation>> extractJoinCondition(
            List<ExactRelation> tableSources) {
        if (compOp.equals("=")) {
            if (left instanceof ColNameExpr && right instanceof ColNameExpr) {
                ExactRelation r1 = findSourceContaining(tableSources, (ColNameExpr) left);
                ExactRelation r2 = findSourceContaining(tableSources, (ColNameExpr) right);
                String leftOriginalName = (r1 != null) ? getOriginalTableName(tableSources, r1.getAlias()) : null;
                String rightOriginalName = (r2 != null) ? getOriginalTableName(tableSources, r2.getAlias()) : null;
                if (r2 != null && leftOriginalName != null && rightOriginalName != null &&
                        !(leftOriginalName.equals(rightOriginalName) &&
                                r1.getAlias().equals(r2.getAlias()))) {
                    return Pair.of((Cond) this, Pair.of(r1, r2));
                }
            } else if (left instanceof ColNameExpr && right instanceof BinaryOpExpr) {
                BinaryOpExpr binaryOp = (BinaryOpExpr) right;
                Expr colRight = null;
                if (binaryOp.getLeft() instanceof ColNameExpr) {
                    if (binaryOp.getRight() instanceof ConstantExpr) {
                        colRight = binaryOp.getLeft();
                    }
                } else if (binaryOp.getRight() instanceof ColNameExpr) {
                    if (binaryOp.getLeft() instanceof ConstantExpr) {
                        colRight = binaryOp.getRight();
                    }
                }
                if (colRight != null) {
                    ExactRelation r1 = findSourceContaining(tableSources, (ColNameExpr) left);
                    ExactRelation r2 = findSourceContaining(tableSources, (ColNameExpr) colRight);

                    String leftOriginalName = (r1 != null) ? getOriginalTableName(tableSources, r1.getAlias()) : null;
                    String rightOriginalName = (r2 != null) ? getOriginalTableName(tableSources, r2.getAlias()) : null;
                    if (r2 != null && leftOriginalName != null && rightOriginalName != null &&
                            !(leftOriginalName.equals(rightOriginalName) &&
                                    r1.getAlias().equals(r2.getAlias()))) {
                        return Pair.of((Cond) this, Pair.of(r1, r2));
                    }
                }
            } else if (left instanceof BinaryOpExpr && right instanceof ColNameExpr) {
                BinaryOpExpr binaryOp = (BinaryOpExpr) left;
                Expr colLeft = null;
                if (binaryOp.getLeft() instanceof ColNameExpr) {
                    if (binaryOp.getRight() instanceof ConstantExpr) {
                        colLeft = binaryOp.getLeft();
                    }
                } else if (binaryOp.getRight() instanceof ColNameExpr) {
                    if (binaryOp.getLeft() instanceof ConstantExpr) {
                        colLeft = binaryOp.getRight();
                    }
                }
                if (colLeft != null) {
                    ExactRelation r1 = findSourceContaining(tableSources, (ColNameExpr) colLeft);
                    ExactRelation r2 = findSourceContaining(tableSources, (ColNameExpr) right);

                    String leftOriginalName = (r1 != null) ?
                            getOriginalTableName(tableSources, r1.getAlias()) : null;
                    String rightOriginalName = (r2 != null) ?
                            getOriginalTableName(tableSources, r2.getAlias()) : null;

                    if (r2 != null && leftOriginalName != null && rightOriginalName != null &&
                            !(leftOriginalName.equals(rightOriginalName) &&
                                    r1.getAlias().equals(r2.getAlias()))) {
                        return Pair.of((Cond) this, Pair.of(r1, r2));
                    }
                }
            } else if (left instanceof BinaryOpExpr && right instanceof BinaryOpExpr) {
                BinaryOpExpr binaryOp = (BinaryOpExpr) left;
                Expr colRight = null;
                Expr colLeft = null;
                if (binaryOp.getLeft() instanceof ColNameExpr) {
                    if (binaryOp.getRight() instanceof ConstantExpr) {
                        colLeft = binaryOp.getLeft();
                    }
                } else if (binaryOp.getRight() instanceof ColNameExpr) {
                    if (binaryOp.getLeft() instanceof ConstantExpr) {
                        colLeft = binaryOp.getRight();
                    }
                }
                binaryOp = (BinaryOpExpr) right;
                if (binaryOp.getLeft() instanceof ColNameExpr) {
                    if (binaryOp.getRight() instanceof ConstantExpr) {
                        colRight = binaryOp.getLeft();
                    }
                } else if (binaryOp.getRight() instanceof ColNameExpr) {
                    if (binaryOp.getLeft() instanceof ConstantExpr) {
                        colRight = binaryOp.getRight();
                    }
                }
                if (colLeft != null && colRight != null) {
                    ExactRelation r1 = findSourceContaining(tableSources, (ColNameExpr) colLeft);
                    ExactRelation r2 = findSourceContaining(tableSources, (ColNameExpr) colRight);

                    String leftOriginalName = (r1 != null) ?
                            getOriginalTableName(tableSources, r1.getAlias()) : null;
                    String rightOriginalName = (r2 != null) ?
                            getOriginalTableName(tableSources, r2.getAlias()) : null;
                    if (r2 != null && leftOriginalName != null && rightOriginalName != null &&
                            !(leftOriginalName.equals(rightOriginalName) &&
                                    r1.getAlias().equals(r2.getAlias()))) {
                        return Pair.of((Cond) this, Pair.of(r1, r2));
                    }
                }
            }
        }
        return null;
    }

    private String getOriginalTableName(List<ExactRelation> tableSources, String alias) {
        for (ExactRelation r : tableSources) {
            String name = getOriginalTableName(r, alias);
            if (name != null) return name;
        }
        return null;
    }

    private String getOriginalTableName(ExactRelation r, String alias) {
        if (r instanceof SingleRelation) {
            if (r.getAlias().equals(alias)) {
                SingleRelation sr = (SingleRelation) r;
                return sr.getTableName().fullyQuantifiedName();
            }
        }
        else if (r instanceof JoinedRelation) {
            String result = getOriginalTableName(((JoinedRelation) r).getLeftSource(), alias);
            if (result == null) {
                result = getOriginalTableName(((JoinedRelation) r).getRightSource(), alias);
            }
            return result;
        }
        else if (r instanceof ProjectedRelation) {
            ProjectedRelation pr = (ProjectedRelation) r;
            if (pr.getAlias().equals(alias)) {
                if (pr.getName() != null) {
                    return pr.getName();
                } else {
                    return alias;
                }
            }
            return getOriginalTableName(pr.getSource(), alias);
        }
        else if (r instanceof AggregatedRelation) {
            AggregatedRelation ar = (AggregatedRelation) r;
            if (ar.getAlias().equals(alias)) {
                if (ar.getName() != null) {
                    return ar.getName();
                } else {
                    return alias;
                }
            }
            return getOriginalTableName(ar.getSource(), alias);
        }
        else if (r instanceof SetRelation) {
            SetRelation sr = (SetRelation) r;
            if (sr.getAlias().equals(alias)) {
                return sr.getName();
            }
            String result = getOriginalTableName(((SetRelation) r).getLeftSource(), alias);
            if (result == null) {
                result = getOriginalTableName(((SetRelation) r).getRightSource(), alias);
            }
            return result;
        }
        return null;
    }

    private ExactRelation findSingleRelation(ExactRelation r, String tab) {
        if (r instanceof SingleRelation) {
            if (r.getAlias().equals(tab)) {
                return r;
            }
        } else if (r instanceof JoinedRelation) {
            JoinedRelation jr = (JoinedRelation) r;
            ExactRelation res = this.findSingleRelation(jr.getLeftSource(), tab);
            if (res != null) return res;
            else return this.findSingleRelation(jr.getRightSource(), tab);
        } else if (r instanceof SetRelation) {
            SetRelation sr = (SetRelation) r;
            ExactRelation res = this.findSingleRelation(sr.getLeftSource(), tab);
            if (res != null) return res;
            else return this.findSingleRelation(sr.getRightSource(), tab);
        }
        return null;
    }


    private ExactRelation findSourceContaining(List<ExactRelation> tableSources, String tab) {
        for (ExactRelation r : tableSources) {
            if (doesRelationContain(r, tab)) {
                return r;
            }
        }
        return null;
    }

    private boolean doesRelationContain(ExactRelation r, String tab) {
        if (r.getAlias().equals(tab)) {
            return true;
        }
        if (r instanceof JoinedRelation) {
            return doesRelationContain(((JoinedRelation) r).getLeftSource(), tab)
                    || doesRelationContain(((JoinedRelation) r).getRightSource(), tab);
        } else if (r instanceof SetRelation) {
            return doesRelationContain(((SetRelation) r).getLeftSource(), tab)
                    || doesRelationContain(((SetRelation) r).getRightSource(), tab);
        }
        return false;
    }

    private ExactRelation findSourceContaining(List<ExactRelation> tableSources, ColNameExpr col) {
        for (ExactRelation r : tableSources) {
            if (doesRelationContain(r, col)) {
                return r;
            }
        }
        return null;
    }

    private boolean doesRelationContain(ExactRelation r, ColNameExpr col) {
        String tab = col.getTab();
        String colName = col.getCol();
        if (tab != null) {
            if (r.getAlias().equals(tab)) {
                return true;
            }
        } else {
            List<SelectElem> selectElemList = r.getSelectElemList();
            if (selectElemList != null) {
                for (SelectElem elem : selectElemList) {
                    if (elem.aliasPresent()) {
                        if (elem.getAlias().equals(colName)) {
                            return true;
                        }
                    } else {
                        if (elem.toString().equals(colName)) {
                            return true;
                        }
                    }
                }
            }
        }
        if (r instanceof JoinedRelation) {
            return doesRelationContain(((JoinedRelation) r).getLeftSource(), col)
                    || doesRelationContain(((JoinedRelation) r).getRightSource(), col);
        } else if (r instanceof SetRelation) {
            return doesRelationContain(((SetRelation) r).getLeftSource(), col)
                    || doesRelationContain(((SetRelation) r).getRightSource(), col);
        }
        return false;
    }

    // private String searchForTableName(VerdictContext vc, ColNameExpr col,
    // List<TableUniqueName> among) {
    // if (col.getTab() != null) {
    // return col.getTab();
    // } else {
    // for (TableUniqueName t : among) {
    // Set<String> cols = vc.getMeta().getColumns(t);
    // if (cols.contains(col.getCol())) {
    // return t.
    // }
    // }
    // }
    // }

    @Override
    public boolean equals(Object a) {
        if (a instanceof CompCond) {
            if (((CompCond) a).getLeft().equals(left) && ((CompCond) a).getRight().equals(right)
                    && ((CompCond) a).getOp().equals(compOp)) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public Cond remove(Cond j) {
        if (equals(j)) {
            return null;
        } else {
            return this;
        }
    }

    @Override
    public Cond withTableSubstituted(String newTab) {
        return new CompCond(left.withTableSubstituted(newTab), compOp, right.withTableSubstituted(newTab));
    }

    @Override
    public Cond withNewTablePrefix(String newPrefix) {
        return new CompCond(left.withNewTablePrefix(newPrefix), compOp, right.withNewTablePrefix(newPrefix));
    }

    @Override
    public String toSql() {
        return String.format("%s %s %s", left.toSql(), compOp, right.toSql());
    }

    @Override
    public boolean equals(Cond o) {
        if (o instanceof CompCond) {
//            return getOp().equals(((CompCond) o).getOp()) && getLeft().equals(((CompCond) o).getLeft())
//                    && getRight().equals(((CompCond) o).getRight());
            // dyoon: previous method was problematic when CompCond references a subquery, where its
            // schema was set to null. Changes have been made to look at the actual query string
            // instead.
            return this.toString().equals(o.toString());
        }
        return false;
    }
}
