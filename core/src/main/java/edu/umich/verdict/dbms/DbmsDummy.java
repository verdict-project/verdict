package edu.umich.verdict.dbms;

import java.sql.Connection;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class DbmsDummy extends Dbms {

	public DbmsDummy(VerdictContext vc) throws VerdictException {
		super(vc, "", "", "", "dummySchema", "", "", "");
	}
	
	@Override
	protected Connection makeDbmsConnection(String url, String className) throws VerdictException  {
		return conn;
	}
	
}
