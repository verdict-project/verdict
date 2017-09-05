package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class CreateViewAsSelectQuery extends Query {

    public CreateViewAsSelectQuery(VerdictContext vc, String q) {
        super(vc, q);
    }

    @Override
    public void compute() throws VerdictException {
        VerdictSQLParser p = StringManipulations.parserOf(queryString);
        Relation r = ExactRelation.from(vc, p.create_view().select_statement());
        p.reset();
        String viewName = p.create_view().view_name().getText();
        
        p.reset();
        boolean exact = p.create_view().select_statement().EXACT() != null;
        if (!exact) {
            r = ((ExactRelation) r).approx();
        }
        
        // compose a query.
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE VIEW ");
        sql.append(viewName);
        sql.append(" AS ");
        
        String selectSql = r.toSql();
        VerdictLogger.debug(this, String.format("Creates a view (%s) with the following query:", viewName));
        VerdictLogger.debugPretty(this, Relation.prettyfySql(vc, selectSql), "  ");
        sql.append(selectSql);
        
        vc.getDbms().executeUpdate(sql.toString());
    }
    
}
