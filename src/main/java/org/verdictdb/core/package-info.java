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

/**
 * Core querying and scramling logics of VerdictDB.
 *
 * <p>Currently active subpackages:
 *
 * <ol>
 *   <li>connection
 *   <li>execplan
 *   <li>querying
 *   <li>scrambling
 *   <li>sqlobject
 * </ol>
 *
 * <p>Notes on partitioning:
 *
 * <ol>
 *   <li>Not always true: When scrambling, we first create partitioned tables; then, insert values.
 *       This is the due to the restrictions of existing DBMS (e.g., Hive, Postgres) which prohibits
 *       creating a partitioned table as a result of a select query. For Hive, see
 *       https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTableCreate/Drop/TruncateTable
 *   <li>Postgres supports dynamic data insertions into appropriate partitions:
 *       https://www.postgresql.org/docs/10/static/sql-insert.html
 *   <li>Hive supports dynamic data insertions into appropriate partitions when options are turned
 *       on.
 * </ol>
 */
package org.verdictdb.core;
