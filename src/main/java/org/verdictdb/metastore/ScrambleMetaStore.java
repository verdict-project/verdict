package org.verdictdb.metastore;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.CreateTableStatement;
import org.verdictdb.core.sqlobject.DropTableQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlwriter.QueryToSql;

public class ScrambleMetaStore extends VerdictMetaStore {
  
  private DbmsConnection conn;
  
  private String storeSchema;
  
  private final String SCHEMA_COLUMN = "schema";
  
  private final String TABLE_COLUMN = "table";
  
  private final String DATA_COLUMN = "data";
  
  public ScrambleMetaStore(DbmsConnection conn, String storeSchema) {
    this.conn = conn;
    this.storeSchema = storeSchema;
  }
  
  public String getStoreSchema() {
    return storeSchema;
  }
  
  public void addToStore(ScrambleMeta scrambleMeta) {
    ScrambleMetaSet scrambleMetaSet = new ScrambleMetaSet();
    scrambleMetaSet.addScrambleMeta(scrambleMeta);
  }
  
  public void addToStore(ScrambleMetaSet scrambleMetaSet) throws VerdictDBException {
    // retrieve
    ScrambleMetaSet previous = retrieve();
    
    // add entries
    for (ScrambleMeta meta : scrambleMetaSet) {
      previous.addScrambleMeta(meta);
    }
    
    // store back
    store(previous);
  }
  
  /**
   * This will replace the existing entry.
   * @param scrambleMetaSet
   * @throws VerdictDBException 
   */
  private void store(ScrambleMetaSet scrambleMetaSet) throws VerdictDBException {
    String jsonData = null;
    SelectQuery dataQuery = SelectQuery.create(ConstantColumn.valueOf("'" + jsonData + "'"));
    
    // create a new table overwriting an existing table.
    CreateTableAsSelectQuery createQuery = 
        new CreateTableAsSelectQuery(SCHEMA_COLUMN, TABLE_COLUMN, dataQuery);
    createQuery.setOverwrite(true);
    String sql = QueryToSql.convert(conn.getSyntax(), createQuery);
    conn.execute(sql);
    
//    // drop the existing table
//    DropTableQuery drop = new DropTableQuery(SCHEMA_COLUMN, TABLE_COLUMN);
//    String dropsql = QueryToSql.convert(conn.getSyntax(), drop); 
//    conn.execute(dropsql);
  }
  
//  CreateTableStatement createScrambleMetaStoreTableStatement() {
//    // create table
//    CreateTableStatement query = new CreateTableStatement();
//    query.setSchemaName(storeSchema);
//    query.setTableName(getMetaStoreTableName());
//    query.addColumnNameAndType(Pair.of(SCHEMA_COLUMN, "VARCHAR(100)"));
//    query.addColumnNameAndType(Pair.of(TABLE_COLUMN, "VARCHAR(100)"));
//    query.addColumnNameAndType(Pair.of(DATA_COLUMN, "TEXT"));
//    query.setIfNotExists(true);
//    return query;
//  }
  
  public ScrambleMetaSet retrieve() {
    return null;
  }

}
