/*
 *    Copyright 2017 University of Michigan
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

package org.verdictdb.core.sqlobject;

/**
 * PosgreSQL: CREATE TABLE newtable (LIKE sourcetable);
 * https://www.postgresql.org/docs/10/static/sql-createtable.html
 *
 * <p>Hive: CREATE TABLE newtable LIKE sourcetable;
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTableLike
 *
 * @author Yongjoo Park
 */
public class CreateTableLikeQuery {}
