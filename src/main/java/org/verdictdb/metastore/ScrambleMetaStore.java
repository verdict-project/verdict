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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.commons.VerdictTimestamp;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.coordinator.VerdictSingleResultFromDbmsQueryResult;
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

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ScrambleMetaStore extends VerdictMetaStore {

  private static final String DEFAULT_STORE_SCHEMA = "verdictdbmetadata";

  private static final String ORIGINAL_SCHEMA_COLUMN = "original_schema";

  private static final String ORIGINAL_TABLE_COLUMN = "original_table";

  private static final String SCRAMBLE_SCHEMA_COLUMN = "scramble_schema";

  private static final String SCRAMBLE_TABLE_COLUMN = "scramble_table";

  private static final String SCRAMBLE_METHOD_COLUMN = "scramble_method";

  private static final String ADDED_AT_COLUMN = "added_at";

  private static final String DATA_COLUMN = "data";

  private static final String DELETED = "DELETED";

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

  public VerdictSingleResult showScrambles() throws VerdictDBException {
    String tableAlias = "t";
    SelectQuery query =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new BaseColumn(tableAlias, ORIGINAL_SCHEMA_COLUMN),
                new BaseColumn(tableAlias, ORIGINAL_TABLE_COLUMN),
                new BaseColumn(tableAlias, SCRAMBLE_SCHEMA_COLUMN),
                new BaseColumn(tableAlias, SCRAMBLE_TABLE_COLUMN),
                new BaseColumn(tableAlias, ADDED_AT_COLUMN),
                new BaseColumn(tableAlias, DATA_COLUMN)),
            new BaseTable(storeSchema, METASTORE_TABLE_NAME, tableAlias));
    query.addOrderby(new OrderbyAttribute(ADDED_AT_COLUMN, "desc"));
    String sql = QueryToSql.convert(conn.getSyntax(), query);
    DbmsQueryResult result = conn.execute(sql);

    return new VerdictSingleResultFromDbmsQueryResult(result);
  }

  public void dropAllScrambleTable(BaseTable originalTable) throws VerdictDBException {
    String originalTableSchema = originalTable.getSchemaName();
    String originalTableName = originalTable.getTableName();
    ScrambleMetaSet metaSet = retrieve();

    for (Iterator<ScrambleMeta> it = metaSet.iterator(); it.hasNext(); ) {
      ScrambleMeta meta = it.next();
      if (meta.getOriginalTableName().equals(originalTableName)
          && meta.getOriginalSchemaName().equals(originalTableSchema)) {
        String scrambleTableSchema = meta.getSchemaName();
        String scrambleTableName = meta.getTableName();
        this.dropScrambleTable(
            new BaseTable(originalTableSchema, originalTableName),
            new BaseTable(scrambleTableSchema, scrambleTableName));
      }
    }
  }

  public void dropScrambleTable(BaseTable originalTable, BaseTable scrambleTable)
      throws VerdictDBException {
    String originalTableSchema = (originalTable != null) ? originalTable.getSchemaName() : "N/A";
    String originalTableName = (originalTable != null) ? originalTable.getTableName() : "N/A";
    String scrambleTableSchema = scrambleTable.getSchemaName();
    String scrambleTableName = scrambleTable.getTableName();

    // if schema of scramble table is not given, set it to default.
    if (scrambleTableSchema.isEmpty()) {
      scrambleTableSchema = conn.getDefaultSchema();
    }

    // drop the actual scrambled table first.
    DropTableQuery dropQuery = new DropTableQuery(scrambleTableSchema, scrambleTableName);
    dropQuery.setIfExists(true);
    String sql = QueryToSql.convert(conn.getSyntax(), dropQuery);
    conn.execute(sql);

    // update metadata with a new row where 'data' and 'method' marked as 'DELETED'
    InsertValuesQuery insertQuery = new InsertValuesQuery();
    insertQuery.setSchemaName(getStoreSchema());
    insertQuery.setTableName(getMetaStoreTableName());
    VerdictTimestamp timestamp = new VerdictTimestamp(new Date());
    insertQuery.setValues(
        Arrays.<Object>asList(
            originalTableSchema,
            originalTableName,
            scrambleTableSchema,
            scrambleTableName,
            DELETED,
            timestamp,
            DELETED));
    sql = QueryToSql.convert(conn.getSyntax(), insertQuery);
    conn.execute(sql);
  }

  /**
   * Removes the metastore table if exists.
   *
   * @throws VerdictDBException
   */
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
   * This will add on top of existing entries. A new metastore table is created if not exists.
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
    VerdictTimestamp timestamp = new VerdictTimestamp(new Date());
    //    System.out.println("new date: " + timestamp);
    //    System.out.println("calendar: " + Calendar.getInstance().get(Calendar.MILLISECOND));
    String jsonString = meta.toJsonString();
    query.setValues(
        Arrays.<Object>asList(
            originalSchema,
            originalTable,
            scrambleSchema,
            scrambleTable,
            scrambleMethod,
            timestamp,
            jsonString));
    return query;
  }

  /**
   * Return scramble meta for an existing scramble
   *
   * @param schema schema for an existing scramble
   * @param table table name for an existing scramble
   * @return ScrambleMeta object for an existing scramble
   */
  public ScrambleMeta retrieveExistingScramble(String schema, String table) {

    try {
      String storeTable = METASTORE_TABLE_NAME;

      List<String> existingSchemas = conn.getSchemas();
      if (!existingSchemas.contains(storeSchema)) {
        return null;
      }

      List<String> existingTables = conn.getTables(storeSchema);
      if (!existingTables.contains(storeTable)) {
        return null;
      }

      // now ready to retrieve
      String tableAlias = "t";
      SelectQuery query =
          SelectQuery.create(
              Arrays.<SelectItem>asList(
                  new BaseColumn(tableAlias, ORIGINAL_SCHEMA_COLUMN),
                  new BaseColumn(tableAlias, ORIGINAL_TABLE_COLUMN),
                  new BaseColumn(tableAlias, SCRAMBLE_SCHEMA_COLUMN),
                  new BaseColumn(tableAlias, SCRAMBLE_TABLE_COLUMN),
                  new BaseColumn(tableAlias, ADDED_AT_COLUMN),
                  new BaseColumn(tableAlias, DATA_COLUMN)),
              new BaseTable(storeSchema, storeTable, tableAlias));
      query.addOrderby(new OrderbyAttribute(ADDED_AT_COLUMN, "desc"));
      String sql = QueryToSql.convert(conn.getSyntax(), query);
      DbmsQueryResult result = conn.execute(sql);

      while (result.next()) {
        String scrambleSchema = result.getString(2);
        String scrambleTable = result.getString(3);
        Pair<String, String> pair = ImmutablePair.of(scrambleSchema, scrambleTable);
        String jsonString = result.getString(5);
        if (scrambleSchema.equals(schema) && scrambleTable.equals(table)) {
          return jsonString.toUpperCase().equals(DELETED)
              ? null
              : ScrambleMeta.fromJsonString(jsonString);
        }
      }
    } catch (VerdictDBException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Retrieve all scramble metadata
   *
   * @return a set of scramble meta
   */
  @Override
  public ScrambleMetaSet retrieve() {
    return retrieve(conn, getStoreSchema());
  }

  /**
   * Static version of retrieve()
   *
   * @return a set of scramble meta
   */
  public static ScrambleMetaSet retrieve(DbmsConnection conn, String storeSchema) {
    ScrambleMetaSet retrieved = new ScrambleMetaSet();

    try {
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
                  new BaseColumn(tableAlias, ORIGINAL_SCHEMA_COLUMN),
                  new BaseColumn(tableAlias, ORIGINAL_TABLE_COLUMN),
                  new BaseColumn(tableAlias, SCRAMBLE_SCHEMA_COLUMN),
                  new BaseColumn(tableAlias, SCRAMBLE_TABLE_COLUMN),
                  new BaseColumn(tableAlias, ADDED_AT_COLUMN),
                  new BaseColumn(tableAlias, DATA_COLUMN)),
              new BaseTable(storeSchema, storeTable, tableAlias));
      query.addOrderby(new OrderbyAttribute(ADDED_AT_COLUMN, "desc"));
      String sql = QueryToSql.convert(conn.getSyntax(), query);
      DbmsQueryResult result = conn.execute(sql);

      Set<Pair<String, String>> deletedSet = new HashSet<>();
      Set<Pair<String, String>> addedSet = new HashSet<>();

      while (result.next()) {
        //        String originalSchema = result.getString(0);
        //        String originalTable = result.getString(1);
        String scrambleSchema = result.getString(2);
        String scrambleTable = result.getString(3);
        //        BaseTable original = new BaseTable(originalSchema, originalTable);
        //        BaseTable scramble = new BaseTable(scrambleSchema, scrambleTable);
        Pair<String, String> pair = ImmutablePair.of(scrambleSchema, scrambleTable);
        String timestamp = result.getString(4);
        //        System.out.println("added time: " + timestamp);
        String jsonString = result.getString(5);
        if (jsonString.toUpperCase().equals(DELETED)) {
          deletedSet.add(pair);
        }
        // skip the scrambled table has been deleted
        if (deletedSet.contains(pair)) {
          continue;
        }
        if (addedSet.contains(pair)) {
          continue;
        }
        ScrambleMeta meta = ScrambleMeta.fromJsonString(jsonString);
        retrieved.addScrambleMeta(meta);

        addedSet.add(pair);
      }
    } catch (VerdictDBException e) {
      e.printStackTrace();
    }

    return retrieved;
  }
}
