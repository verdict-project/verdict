package org.verdictdb.metastore;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.querying.CreateSchemaQuery;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.CreateTableDefinitionQuery;
import org.verdictdb.core.sqlobject.InsertValuesQuery;
import org.verdictdb.core.sqlobject.OrderbyAttribute;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlwriter.QueryToSql;

public class ScrambleMetaStore extends VerdictMetaStore {
  
  private final static String DEFAULT_STORE_SCHEMA = "verdictdbmetadata";

  private final static String ADDED_AT_COLUMN = "addedat";

  private final static String SCHEMA_COLUMN = "schema";

  private final static String TABLE_COLUMN = "table";

  private final static String DATA_COLUMN = "data";

  private DbmsConnection conn;

  private String storeSchema = DEFAULT_STORE_SCHEMA;
  
  public ScrambleMetaStore(DbmsConnection conn) {
    this(conn, DEFAULT_STORE_SCHEMA);
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
  
  public static String getSchemaColumn() {
    return SCHEMA_COLUMN;
  }
  
  public static String getTableColumn() {
    return TABLE_COLUMN;
  }
  
  public static String getDataColumn() {
    return DATA_COLUMN;
  }

  public void addToStore(ScrambleMeta scrambleMeta) throws VerdictDBException {
    ScrambleMetaSet scrambleMetaSet = new ScrambleMetaSet();
    scrambleMetaSet.addScrambleMeta(scrambleMeta);
    addToStore(scrambleMetaSet);
  }

  /**
   * This will add on top of existing entries.
   * @param scrambleMetaSet
   * @throws VerdictDBException 
   */
  public void addToStore(ScrambleMetaSet scrambleMetaSet) throws VerdictDBException {
    
    // create a schema if not exists
    CreateSchemaQuery createSchemaQuery = new CreateSchemaQuery(storeSchema);
    createSchemaQuery.setIfNotExists(true);
    String sql = QueryToSql.convert(conn.getSyntax(), createSchemaQuery);
    conn.execute(sql);

    // create a new table if not exists
    CreateTableDefinitionQuery createTableQuery = createScrambleMetaStoreTableStatement();
    sql = QueryToSql.convert(conn.getSyntax(), createTableQuery);
    conn.execute(sql);

    // insert a new entry
    StringBuilder insertSqls = new StringBuilder();
    for (ScrambleMeta meta : scrambleMetaSet) {
      InsertValuesQuery q = createInsertMetaQuery(meta);
      String s = QueryToSql.convert(conn.getSyntax(), q);
      insertSqls.append(s);
      insertSqls.append("; ");
    }
    conn.execute(insertSqls.toString());
  }

  private CreateTableDefinitionQuery createScrambleMetaStoreTableStatement() {
    // create table
    CreateTableDefinitionQuery query = new CreateTableDefinitionQuery();
    query.setSchemaName(storeSchema);
    query.setTableName(getMetaStoreTableName());
    query.addColumnNameAndType(Pair.of(ADDED_AT_COLUMN, "TIMESTAMP"));
    query.addColumnNameAndType(Pair.of(SCHEMA_COLUMN, "VARCHAR(100)"));
    query.addColumnNameAndType(Pair.of(TABLE_COLUMN, "VARCHAR(100)"));
    query.addColumnNameAndType(Pair.of(DATA_COLUMN, "TEXT"));
    query.setIfNotExists(true);
    return query;
  }

  private InsertValuesQuery createInsertMetaQuery(ScrambleMeta meta) {
    InsertValuesQuery query = new InsertValuesQuery();
    query.setSchemaName(getStoreSchema());
    query.setTableName(getMetaStoreTableName());

    String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    String scrambleSchema = meta.getSchemaName();
    String scrambleTable = meta.getTableName();
    String jsonString = meta.toJsonString();
    query.setValues(Arrays.<Object>asList(timeStamp, scrambleSchema, scrambleTable, jsonString));
    return query;
  }

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
      SelectQuery query = SelectQuery.create(
          Arrays.<SelectItem>asList(
              new BaseColumn(tableAlias, ADDED_AT_COLUMN),
              new BaseColumn(tableAlias, SCHEMA_COLUMN),
              new BaseColumn(tableAlias, TABLE_COLUMN),
              new BaseColumn(tableAlias, DATA_COLUMN)), 
          new BaseTable(storeSchema, storeTable, tableAlias));
      query.addOrderby(new OrderbyAttribute(ADDED_AT_COLUMN));
      String sql = QueryToSql.convert(conn.getSyntax(), query);
      DbmsQueryResult result = conn.execute(sql);

      while(result.next()) {
        //      String schema = result.getString(1);
        //      String table = result.getString(2);
        String jsonString = result.getString(3);
        ScrambleMeta meta = ScrambleMeta.fromJsonString(jsonString);
        retrieved.addScrambleMeta(meta);
      }
    } catch (VerdictDBException e) {
      e.printStackTrace();
    }

    return retrieved;
  }

}
