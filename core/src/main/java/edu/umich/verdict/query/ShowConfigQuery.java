package edu.umich.verdict.query;

import java.util.Map;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

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
		vc.getDbms().execute("select 1");	// dummy
	}

}
