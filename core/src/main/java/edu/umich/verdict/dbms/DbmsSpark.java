package edu.umich.verdict.dbms;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class DbmsSpark extends Dbms {
	
	static String DBNAME = "Spark";

	protected DbmsSpark(VerdictContext vc) throws VerdictException {	
		super(vc, DBNAME);
	}

}
