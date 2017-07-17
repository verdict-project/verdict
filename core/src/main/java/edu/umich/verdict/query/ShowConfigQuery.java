package edu.umich.verdict.query;

import java.util.Map;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StringManipulations;

public class ShowConfigQuery extends Query {

	public ShowConfigQuery(VerdictContext vc, String q) {
		super(vc, q);
	}
	
	@Override
	public void compute() throws VerdictException {
		Map<String, String> conf = vc.getConf().getConfigs();
		for (Map.Entry<String, String> kv : conf.entrySet()) {
			String key = kv.getKey();
			String value = kv.getValue();
			System.out.println(key + "\t" + value);
		}
	}

}
