/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.commons;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

/** Created by Dong Young Yoon on 8/9/18. */
public class VerdictOptionTest {
  @Test
  public void connectionStringParseTest1() {
    VerdictOption option = new VerdictOption();
    String jdbcConnectionString =
        "jdbc:db://localhost:3306?verdictdbmetaschema=mymeta&verdictdbtempschema=mytemp";

    assertEquals(VerdictOption.getDefaultMetaSchemaName(), option.getVerdictMetaSchemaName());
    assertEquals(VerdictOption.getDefaultTempSchemaName(), option.getVerdictTempSchemaName());

    option.parseConnectionString(jdbcConnectionString);

    assertEquals("mymeta", option.getVerdictMetaSchemaName());
    assertEquals("mytemp", option.getVerdictTempSchemaName());
  }

  @Test
  public void connectionStringParseTest2() {
    VerdictOption option = new VerdictOption();
    String jdbcConnectionString =
        "jdbc:db://localhost:3306;verdictdbmetaschema=mymeta;verdictdbtempschema=mytemp";

    assertEquals(VerdictOption.getDefaultMetaSchemaName(), option.getVerdictMetaSchemaName());
    assertEquals(VerdictOption.getDefaultTempSchemaName(), option.getVerdictTempSchemaName());

    option.parseConnectionString(jdbcConnectionString);

    assertEquals("mymeta", option.getVerdictMetaSchemaName());
    assertEquals("mytemp", option.getVerdictTempSchemaName());
  }

  @Test
  public void propertiesParseTest() {
    VerdictOption option = new VerdictOption();

    assertEquals(VerdictOption.getDefaultMetaSchemaName(), option.getVerdictMetaSchemaName());
    assertEquals(VerdictOption.getDefaultTempSchemaName(), option.getVerdictTempSchemaName());

    Properties prop = new Properties();
    prop.setProperty("verdictdbmetaschema", "mymeta");
    prop.setProperty("verdictdbtempschema", "mytemp");

    option.parseProperties(prop);

    assertEquals("mymeta", option.getVerdictMetaSchemaName());
    assertEquals("mytemp", option.getVerdictTempSchemaName());
  }
}
