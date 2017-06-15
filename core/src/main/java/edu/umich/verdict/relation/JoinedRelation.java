package edu.umich.verdict.relation;

import java.util.List;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.Alias;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.expr.ColNameExpr;

public class JoinedRelation extends ExactRelation {
	
	private ExactRelation source1;
	
	private ExactRelation source2;
	
	private List<ColNameExpr> joinCols;

	public JoinedRelation(VerdictContext vc, ExactRelation source1, ExactRelation source2, List<ColNameExpr> joinCols) {
		super(vc, TableUniqueName.uname(vc, Alias.genDerivedTableAlias(0).toString()));
		this.source1 = source1;
		this.source2 = source2;
		this.joinCols = joinCols;
	}

}
