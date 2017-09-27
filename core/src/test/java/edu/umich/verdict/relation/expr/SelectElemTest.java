/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.relation.expr;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.umich.verdict.VerdictContext;

public class SelectElemTest {

    VerdictContext dummyContext = null;

    @Test
    public void starTest() {
        SelectElem.from(dummyContext, "*");
    }

    @Test
    public void aliasTest() {
        SelectElem e = SelectElem.from(dummyContext, "count(*) AS __group_size");
        assertEquals("count(*) AS `__group_size`", e.toString());
    }

    @Test
    public void randExprTest() {
        SelectElem e = SelectElem.from(dummyContext, "mod(rand(unix_timestamp()), 100) * 100 AS __vpart");
        assertEquals("(mod(rand(unix_timestamp()), 100) * 100) AS `__vpart`", e.toString());
//        System.out.println(e.toString());

        e = SelectElem.from(dummyContext, "(rand(unix_timestamp()) * 100) % 100 AS __vpart");
        assertEquals("((rand(unix_timestamp()) * 100) % 100) AS `__vpart`", e.toString());
//        System.out.println(e.toString());
    }

}
