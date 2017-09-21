/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict;

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import java.util.Set;

import org.junit.Test;

public class ReadJsonTest {

    private static VerdictConf vcNormal = new VerdictConf();
    Properties pNormal = vcNormal.toProperties();
    Set keys = pNormal.keySet();

    @Test
    public void testdefault() {
        VerdictConf vcNested = new VerdictConf("testReading.json");
        for (Object key : keys) {
            String aKey = (String) key;
            assert (vcNested.doesContain(aKey));
            assertEquals(vcNormal.get(aKey), vcNested.get(aKey));
        }
    }

    @Test
    public void testOverwrite() {
        VerdictConf vcMore = new VerdictConf("testOverwrite.json");
        assert (vcMore.doesContain("hive2.port"));
        assertEquals(vcMore.get("hive2.port"), "20000");

        assert (vcMore.doesContain("verdict.bypass"));
        assertEquals(vcMore.get("verdict.bypass"), "yes");

        for (Object key : keys) {
            String aKey = (String) key;
            assert (vcMore.doesContain(aKey));

            if (!aKey.equals("hive2.port") && !aKey.equals("verdict.bypass")) {
                assertEquals(vcNormal.get(aKey), vcMore.get(aKey));
            }
        }
    }
}
