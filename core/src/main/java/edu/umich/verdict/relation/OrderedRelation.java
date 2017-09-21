/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.relation;

import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.OrderByExpr;

public class OrderedRelation extends ExactRelation {

    protected ExactRelation source;

    protected List<OrderByExpr> orderby;

    protected OrderedRelation(VerdictContext vc, ExactRelation source, List<OrderByExpr> orderby) {
        super(vc);
        this.source = source;
        this.orderby = orderby;
        subquery = true;
        this.alias = source.alias;
    }

    @Override
    protected String getSourceName() {
        return getAlias();
    }

    @Override
    protected List<ApproxRelation> nBestSamples(Expr elem, int n) throws VerdictException {
        return source.nBestSamples(elem, n);
    }

    @Override
    public ApproxRelation approx() throws VerdictException {
        ApproxRelation a = new ApproxOrderedRelation(vc, source.approx(), orderby);
        a.setAlias(getAlias());
        return a;
    }

    @Override
    protected ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
        return null;
    }

    @Override
    public String toSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(source.toSql());
        sql.append(" ORDER BY ");
        sql.append(Joiner.on(", ").join(orderby));
        return sql.toString();
    }

    // @Override
    // public List<SelectElem> getSelectList() {
    // return source.getSelectList();
    // }

    @Override
    public List<ColNameExpr> accumulateSamplingProbColumns() {
        return source.accumulateSamplingProbColumns();
    }

    @Override
    protected String toStringWithIndent(String indent) {
        StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("%s(%s) [%s]\n", this.getClass().getSimpleName(), getAlias(),
                Joiner.on(", ").join(orderby)));
        s.append(source.toStringWithIndent(indent + "  "));
        return s.toString();
    }

    // @Override
    // public List<SelectElem> getSelectList() {
    // return source.getSelectList();
    // }

    @Override
    public ColNameExpr partitionColumn() {
        ColNameExpr col = source.partitionColumn();
        col.setTab(getAlias());
        return col;
    }

}
