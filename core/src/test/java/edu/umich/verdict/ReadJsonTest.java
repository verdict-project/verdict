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
	public void testdefault() {
		VerdictConf vcNested = new VerdictConf("testReading.json");
		for (Object key:keys) {
			String aKey = (String) key;
			assert(vcNested.doesContain(aKey));
			assertEquals(vcNormal.get(aKey), vcNested.get(aKey));
		}
	}
	
	@Test
	public void testOverwrite() {
		VerdictConf vcMore = new VerdictConf("testOverwrite.json");
		assert(vcMore.doesContain("hive2.port"));
		assertEquals(vcMore.get("hive2.port"), "20000");
		
		assert(vcMore.doesContain("verdict.bypass"));
		assertEquals(vcMore.get("verdict.bypass"), "yes");
			
		for (Object key:keys) {
			String aKey = (String) key;
			assert(vcMore.doesContain(aKey));
			
			if (!aKey.equals("hive2.port") && !aKey.equals("verdict.bypass")) {
				assertEquals(vcNormal.get(aKey), vcMore.get(aKey));
			}
		}
	}
}
