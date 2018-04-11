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

package edu.umich.verdict.relation;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.SelectElem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Dong Young Yoon on 4/2/18.
 */
public class SetRelation extends ExactRelation {

    private ExactRelation source1;
    private ExactRelation source2;
    private SetType type;

    public enum SetType {UNION_ALL, UNION, EXCEPT, INTERSECT, UNKNOWN}

    public SetRelation(VerdictContext vc, ExactRelation source1, ExactRelation source2,
                          SetRelation.SetType type) {
        super(vc);
        this.source1 = source1;
        this.source2 = source2;
        this.type = type;
    }

    public SetRelation(SetRelation other) {
        super(other.vc);
        this.source1 = other.source1;
        this.source2 = other.source2;
        this.type = other.type;
        this.name = other.name;
    }

    public ExactRelation getLeftSource() {
        return source1;
    }

    public ExactRelation getRightSource() {
        return source2;
    }

    public SetType getSetType() {
        return type;
    }

    private String getSetTypeString() {
        switch (type) {
            case UNION_ALL:
                return "UNION ALL";
            case UNION:
                return "UNION";
            case EXCEPT:
                return "EXCEPT";
            case INTERSECT:
                return "INTERSECT";
            case UNKNOWN:
            default:
                return "UNKNOWN";
        }
    }

    @Override
    protected String getSourceName() {
        return getAlias();
    }

    @Override
    public ApproxRelation approx() throws VerdictException {
        ApproxRelation a = new ApproxSetRelation(vc, source1.approx(), source2.approx(), type);
        a.setAlias(getAlias());
        return a;
    }

    @Override
    protected ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
        ApproxRelation a = new ApproxSetRelation(vc, source1.approxWith(replace),
                source2.approxWith(replace),
                type);
        a.setAlias(getAlias());
        return a;
    }

    @Override
    protected List<ApproxRelation> nBestSamples(Expr elem, int n) throws VerdictException {
        // Now it simply follows the implementation from JoinedRelation.
        List<ApproxRelation> ofSources1 = source1.nBestSamples(elem, n);
        List<ApproxRelation> ofSources2 = source2.nBestSamples(elem, n);
        List<ApproxRelation> sets = new ArrayList<>();

        for (ApproxRelation a1 : ofSources1) {
            for (ApproxRelation a2 : ofSources2) {
                ApproxSetRelation s = new ApproxSetRelation(vc, a1, a2, type);
                sets.add(s);
            }
        }
        return sets;
    }

    @Override
    public ColNameExpr partitionColumn() {
        // Now it simply follows the implementation from JoinedRelation.
        ColNameExpr col1 = source1.partitionColumn();
        if (col1 != null) {
            return col1;
        } else {
            ColNameExpr col2 = source2.partitionColumn();
            return col2;
        }
    }

    @Override
    public List<SelectElem> getSelectElemList() {
        List<SelectElem> selectList = source1.getSelectElemList();
        selectList.addAll(source2.getSelectElemList());
        return selectList;
    }

    @Override
    public List<ColNameExpr> accumulateSamplingProbColumns() {
        // Again, same implementation from JoinedRelation.
        List<ColNameExpr> union = new ArrayList<>(source1.accumulateSamplingProbColumns());
        union.addAll(source2.accumulateSamplingProbColumns());
        return union;
    }

    @Override
    protected String toStringWithIndent(String indent) {
        StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("%s(%s) [%s]\n", this.getClass().getSimpleName(), getAlias(),
                this.getSetTypeString()));
        s.append(source1.toStringWithIndent(indent + "  "));
        s.append(source2.toStringWithIndent(indent + "  "));
        return s.toString();
    }

    @Override
    public String toSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(source1.toSql());
        sql.append(String.format(" %s ", this.getSetTypeString()));
        sql.append(source2.toSql());
        return sql.toString();
    }
}
