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
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.expr.*;

import java.util.*;

/**
 * Created by Dong Young Yoon on 4/3/18.
 */
public class ApproxSetRelation extends ApproxRelation {

    private ApproxRelation source1;
    private ApproxRelation source2;
    private SetRelation.SetType type;

    public ApproxSetRelation(VerdictContext vc, ApproxRelation source1, ApproxRelation source2,
                             SetRelation.SetType type) {
        super(vc);
        this.source1 = source1;
        this.source2 = source2;
        this.type = type;
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
            default:
                return "UNKNOWN";
        }
    }

    @Override
    public ExactRelation rewriteForPointEstimate() {
        ExactRelation s = new SetRelation(vc, source1.rewriteForPointEstimate(),
                source2.rewriteForPointEstimate(),
                type);
        s.setAlias(getAlias());
        return s;
    }

    @Override
    protected ExactRelation rewriteWithPartition() {
        ExactRelation newSource1 = source1.rewriteWithPartition();
        ExactRelation newSource2 = source2.rewriteWithPartition();

        SetRelation s = new SetRelation(vc, newSource1, newSource2, type);
        s.setAlias(getAlias());
        return s;
    }

    @Override
    protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
        List<Expr> expr1 = source1.samplingProbabilityExprsFor(f);
        List<Expr> expr2 = source2.samplingProbabilityExprsFor(f);
        expr1.addAll(expr2);
        return expr1;
    }

    @Override
    public double samplingProbability() {
        switch (type) {
            // For now, let's say that UNION ALL and UNION are roughly same.
            case UNION_ALL:
            case UNION:
                return source1.samplingProbability() + source2.samplingProbability();
            case EXCEPT:
                // This is not true... need to revise it later.
                return source1.samplingProbability();
            case INTERSECT:
                return source1.samplingProbability() * source2.samplingProbability();
            default:
                return 0;
        }
    }

    @Override
    public double cost() {
        return source1.cost() + source2.cost();
    }

    @Override
    public Expr tupleProbabilityColumn() {
        Expr expr1 = source1.tupleProbabilityColumn();
        Expr expr2 = source2.tupleProbabilityColumn();

        switch (type) {
            // For now, let's say that UNION ALL and UNION are roughly same.
            case UNION_ALL:
            case UNION:
                return new BinaryOpExpr(vc, expr1, expr2, "+");
            case EXCEPT:
                // This is not true... need to revise it later.
                return expr1;
            case INTERSECT:
                return new BinaryOpExpr(vc, expr1, expr2, "*");
            default:
                return expr1;
        }
    }

    @Override
    public Expr tableSamplingRatio() {
        Expr expr1 = source1.tableSamplingRatio();
        Expr expr2 = source2.tableSamplingRatio();
        Expr combined = new BinaryOpExpr(vc, expr1, expr2, "*");
        return combined;
    }

    @Override
    public String sampleType() {
        return source1.sampleType() + "-" + source2.sampleType();
    }

    @Override
    protected List<String> getColumnsOnWhichSamplesAreCreated() {
        return Arrays.asList();
    }

    @Override
    protected Map<TableUniqueName, String> tableSubstitution() {
        Map<TableUniqueName, String> sub1 = source1.tableSubstitution();
        Map<TableUniqueName, String> sub2 = source2.tableSubstitution();

        HashMap<TableUniqueName, String> m = new HashMap<TableUniqueName, String>();
        m.putAll(sub1);
        m.putAll(sub2);
        return m;
    }

    @Override
    protected boolean doesIncludeSample() {
        return source1.doesIncludeSample() || source2.doesIncludeSample();
    }

    @Override
    public boolean equals(ApproxRelation o) {
        if (o instanceof ApproxSetRelation) {
            ApproxSetRelation s = (ApproxSetRelation) o;
           if (source1.equals(s.source1) && source2.equals(s.source2)) {
               if (type == s.type) {
                   return true;
               }
           }
        }
        return false;
    }

    @Override
    protected String toStringWithIndent(String indent) {
        StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("%s(%s) [%s], sample type: %s (%s), sampling prob: %f, cost: %f\n",
                this.getClass().getSimpleName(), getAlias(), getSetTypeString(), sampleType(),
                getColumnsOnWhichSamplesAreCreated(), samplingProbability(), cost()));
        s.append(source1.toStringWithIndent(indent + "  "));
        s.append(source2.toStringWithIndent(indent + "  "));
        return s.toString();
    }

    @Override
    public List<ColNameExpr> getAssociatedColumnNames(TableNameExpr tabExpr) {
        List<ColNameExpr> colnames = new ArrayList<>();
        colnames.addAll(source1.getAssociatedColumnNames(tabExpr));
        colnames.addAll(source2.getAssociatedColumnNames(tabExpr));
        return colnames;
    }
}
