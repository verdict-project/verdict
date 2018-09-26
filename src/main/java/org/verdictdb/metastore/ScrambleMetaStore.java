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

package org.verdictdb.metastore;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.CreateSchemaQuery;
import org.verdictdb.core.sqlobject.CreateTableDefinitionQuery;
import org.verdictdb.core.sqlobject.DropTableQuery;
import org.verdictdb.core.sqlobject.InsertValuesQuery;
import org.verdictdb.core.sqlobject.OrderbyAttribute;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlwriter.QueryToSql;

public class ScrambleMetaStore extends VerdictMetaStore {

  private static final String DEFAULT_STORE_SCHEMA = "verdictdbmetadata";

  private static final String ORIGINAL_SCHEMA_COLUMN = "original_schema";

  private static final String ORIGINAL_TABLE_COLUMN = "original_table";

  private static final String SCRAMBLE_SCHEMA_COLUMN = "scramble_schema";

  private static final String SCRAMBLE_TABLE_COLUMN = "scramble_table";

  private static final String SCRAMBLE_METHOD_COLUMN = "scramble_method";

  private static final String ADDED_AT_COLUMN = "added_at";

  private static final String DATA_COLUMN = "data";

  private DbmsConnection conn;

  private String storeSchema;

  private static final VerdictDBLogger LOG = VerdictDBLogger.getLogger(ScrambleMetaStore.class);

  public ScrambleMetaStore(DbmsConnection conn, VerdictOption options) {
    this(conn, options.getVerdictMetaSchemaName());
  }

  public ScrambleMetaStore(DbmsConnection conn, String storeSchema) {
    this.conn = conn;
    this.storeSchema = storeSchema;
  }

  public String getStoreSchema() {
    return storeSchema;
  }

  public static String getDefaultStoreSchema() {
    return DEFAULT_STORE_SCHEMA;
  }

  public static String getAddedAtColumn() {
    return ADDED_AT_COLUMN;
  }

  public static String getOriginalSchemaColumn() {
    return ORIGINAL_SCHEMA_COLUMN;
  }

  public static String getOriginalTableColumn() {
    return ORIGINAL_TABLE_COLUMN;
  }

  public static String getScrambleSchemaColumn() {
    return SCRAMBLE_SCHEMA_COLUMN;
  }

  public static String getScrambleTableColumn() {
    return SCRAMBLE_TABLE_COLUMN;
  }

  public static String getScrambleMethodColumn() {
    return SCRAMBLE_METHOD_COLUMN;
  }

  public static String getDataColumn() {
    return DATA_COLUMN;
  }

  public void addToStore(ScrambleMeta scrambleMeta) throws VerdictDBException {
    ScrambleMetaSet scrambleMetaSet = new ScrambleMetaSet();
    scrambleMetaSet.addScrambleMeta(scrambleMeta);
    addToStore(scrambleMetaSet);
  }

  public void remove() throws VerdictDBException {
    // create a schema if not exists
    CreateSchemaQuery createSchemaQuery = new CreateSchemaQuery(storeSchema);
    createSchemaQuery.setIfNotExists(true);
    String sql = QueryToSql.convert(conn.getSyntax(), createSchemaQuery);
    conn.execute(sql);

    DropTableQuery dropQuery = new DropTableQuery(storeSchema, getMetaStoreTableName());
    dropQuery.setIfExists(true);
    sql = QueryToSql.convert(conn.getSyntax(), dropQuery);
    conn.execute(sql);
  }

  /**
   * This will add on top of existing entries.
   *
   * @param scrambleMetaSet
   * @throws VerdictDBException
   */
  public void addToStore(ScrambleMetaSet scrambleMetaSet) throws VerdictDBException {
    String sql;
    
    // create a schema if not exists
    if (!conn.getSchemas().contains(storeSchema)) {
      CreateSchemaQuery createSchemaQuery = new CreateSchemaQuery(storeSchema);
      createSchemaQuery.setIfNotExists(true);
      sql = QueryToSql.convert(conn.getSyntax(), createSchemaQuery);
      conn.execute(sql);
    }

    // create a new table if not exists
    if (!conn.getTables(storeSchema).contains(getMetaStoreTableName())) {
      CreateTableDefinitionQuery createTableQuery = createScrambleMetaStoreTableStatement();
      sql = QueryToSql.convert(conn.getSyntax(), createTableQuery);
      conn.execute(sql);
    }

    // insert a new entry
    StringBuilder insertSqls = new StringBuilder();
    for (ScrambleMeta meta : scrambleMetaSet) {
      InsertValuesQuery q = createInsertMetaQuery(meta);
      String s = QueryToSql.convert(conn.getSyntax(), q);
      LOG.debug("Adding a new scramble meta entry with the query: {}", s);
      insertSqls.append(s);
      insertSqls.append("; ");
    }
    conn.execute(insertSqls.toString());
  }

  private CreateTableDefinitionQuery createScrambleMetaStoreTableStatement() {
    // create table
    String schemaAndTableColumnType = conn.getSyntax().getGenericStringDataTypeName();
    String addedAtColumnType = "TIMESTAMP";
    String dataColumnType = conn.getSyntax().getGenericStringDataTypeName();

    CreateTableDefinitionQuery query = new CreateTableDefinitionQuery();
    query.setSchemaName(storeSchema);
    query.setTableName(getMetaStoreTableName());
    query.addColumnNameAndType(Pair.of(ORIGINAL_SCHEMA_COLUMN, schemaAndTableColumnType));
    query.addColumnNameAndType(Pair.of(ORIGINAL_TABLE_COLUMN, schemaAndTableColumnType));
    query.addColumnNameAndType(Pair.of(SCRAMBLE_SCHEMA_COLUMN, schemaAndTableColumnType));
    query.addColumnNameAndType(Pair.of(SCRAMBLE_TABLE_COLUMN, schemaAndTableColumnType));
    query.addColumnNameAndType(Pair.of(SCRAMBLE_METHOD_COLUMN, schemaAndTableColumnType));
    query.addColumnNameAndType(Pair.of(ADDED_AT_COLUMN, addedAtColumnType));
    query.addColumnNameAndType(Pair.of(DATA_COLUMN, dataColumnType));
    query.setIfNotExists(true);
    return query;
  }

  private InsertValuesQuery createInsertMetaQuery(ScrambleMeta meta) {
    InsertValuesQuery query = new InsertValuesQuery();
    query.setSchemaName(getStoreSchema());
    query.setTableName(getMetaStoreTableName());

    String originalSchema = meta.getOriginalSchemaName();
    String originalTable = meta.getOriginalTableName();
    String scrambleSchema = meta.getSchemaName();
    String scrambleTable = meta.getTableName();
    String scrambleMethod = meta.getMethod();
    String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    String jsonString = meta.toJsonString();
    query.setValues(
        Arrays.<Object>asList(
            originalSchema,
            originalTable,
            scrambleSchema,
            scrambleTable,
            scrambleMethod,
            timeStamp,
            jsonString));
    return query;
  }

  /**
   * Retrieve all scramble metadata
   *
   * @return a set of scramble meta
   */
  @Override
  public ScrambleMetaSet retrieve() {
    ScrambleMetaSet retrieved = new ScrambleMetaSet();

    try {
      String storeSchema = getStoreSchema();
      String storeTable = getMetaStoreTableName();

      List<String> existingSchemas = conn.getSchemas();
      if (existingSchemas.contains(storeSchema) == false) {
        return new ScrambleMetaSet();
      }

      List<String> existingTables = conn.getTables(storeSchema);
      if (existingTables.contains(storeTable) == false) {
        return new ScrambleMetaSet();
      }

      // now ready to retrieve
      String tableAlias = "t";
      SelectQuery query =
          SelectQuery.create(
              Arrays.<SelectItem>asList(
                  new BaseColumn(tableAlias, ADDED_AT_COLUMN),
                  new BaseColumn(tableAlias, DATA_COLUMN)),
              new BaseTable(storeSchema, storeTable, tableAlias));
      query.addOrderby(new OrderbyAttribute(ADDED_AT_COLUMN));
      String sql = QueryToSql.convert(conn.getSyntax(), query);
      DbmsQueryResult result = conn.execute(sql);

      while (result.next()) {
        String jsonString = result.getString(1);
        ScrambleMeta meta = ScrambleMeta.fromJsonString(jsonString);
        retrieved.addScrambleMeta(meta);
      }
    } catch (VerdictDBException e) {
      e.printStackTrace();
    }

    return retrieved;
  }

  /**
   * Static version of retrieve() that uses default schema and table names
   *
   * @return a set of scramble meta
   */
  public static ScrambleMetaSet retrieve(DbmsConnection conn, VerdictOption options) {
    ScrambleMetaSet retrieved = new ScrambleMetaSet();

    try {
      String storeSchema = options.getVerdictMetaSchemaName();
      String storeTable = METASTORE_TABLE_NAME;

      List<String> existingSchemas = conn.getSchemas();
      if (existingSchemas.contains(storeSchema) == false) {
        return new ScrambleMetaSet();
      }

      List<String> existingTables = conn.getTables(storeSchema);
      if (existingTables.contains(storeTable) == false) {
        return new ScrambleMetaSet();
      }

      // now ready to retrieve
      String tableAlias = "t";
      SelectQuery query =
          SelectQuery.create(
              Arrays.<SelectItem>asList(
                  new BaseColumn(tableAlias, ADDED_AT_COLUMN),
                  new BaseColumn(tableAlias, DATA_COLUMN)),
              new BaseTable(storeSchema, storeTable, tableAlias));
      query.addOrderby(new OrderbyAttribute(ADDED_AT_COLUMN));
      String sql = QueryToSql.convert(conn.getSyntax(), query);
      DbmsQueryResult result = conn.execute(sql);

      while (result.next()) {
        String jsonString = result.getString(1);
        ScrambleMeta meta = ScrambleMeta.fromJsonString(jsonString);
        retrieved.addScrambleMeta(meta);
      }
    } catch (VerdictDBException e) {
      e.printStackTrace();
    }

    return retrieved;
  }
}
