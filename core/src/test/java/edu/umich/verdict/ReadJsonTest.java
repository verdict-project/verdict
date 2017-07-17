package edu.umich.verdict;

import static org.junit.Assert.*;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

public class ReadJsonTest {
	
	private static VerdictConf vcNormal = new VerdictConf();
	Properties pNormal = vcNormal.toProperties();
	Set keys = pNormal.keySet();
	
	@Test
	public void testVerdictConfString() {
		VerdictConf vcNested = new VerdictConf("verdict_conf.json");
		for (Object key:keys) {
			String aKey = (String) key;
			assert(vcNested.doesContain(aKey));
			assertEquals(vcNormal.get(aKey), vcNested.get(aKey));
		}
	}
	
	@Test
	public void testComplicated() {
		VerdictConf vcMore = new VerdictConf("vc_plain.json");
		for (Object key:keys) {
			String aKey = (String) key;
			assert(vcMore.doesContain(aKey));
			assertEquals(vcNormal.get(aKey), vcMore.get(aKey));
		}
	}
	
}
