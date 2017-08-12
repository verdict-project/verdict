package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.relation.ApproxRelation;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.util.StringManipulations;

public class SelectQuery extends Query {


    public SelectQuery(VerdictContext vc, String queryString) {
        super(vc, queryString);
    }
    
    public static Relation queryToRelation(VerdictContext vc, String sql) throws VerdictException {
        ExactRelation r = ExactRelation.from(vc, sql);
        
        VerdictSQLParser p = StringManipulations.parserOf(sql);
        VerdictSQLBaseVisitor<Boolean> visitor = new VerdictSQLBaseVisitor<Boolean>() {
            @Override
            public Boolean visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
                return (ctx.EXACT() != null)? true : false;
            }
        };
        Boolean exact = visitor.visit(p.select_statement());
        
        if (exact) {
            return r;
        } else {
            ApproxRelation a = r.approx();
            return a;
        }
    }

    @Override
    public void compute() throws VerdictException {
        super.compute();
        Relation r = queryToRelation(vc, queryString);
        
        if (vc.getDbms().isJDBC()) {
            rs = r.collectResultSet();
        } else {
            df = r.collectDataFrame();
        }
    }

}
