package edu.umich.verdict.relation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.expr.Expr;

public interface Relation {
	
	public boolean isApproximate();
	
	public List<List<Object>> collect() throws VerdictException;
	
	public ResultSet collectResultSet() throws VerdictException;
	
	public TableUniqueName getTableName();
	
	public boolean isDerivedTable();
	
	public long count() throws VerdictException;
	
	public double sum(Expr expr) throws VerdictException;
	
	public double avg(Expr expr) throws VerdictException;
	
	public long countDistinct(Expr expr) throws VerdictException;
	
//	public long approxCountDistinct(Expr expr) throws VerdictException;

}
