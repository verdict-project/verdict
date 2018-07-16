package org.verdictdb.metastore;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.CreateTableStatement;

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
  
  public void store(ScrambleMetaSet scrambleMetaSet) {
    CreateTableStatement query = createScrambleMetaStoreTableStatement();
    
    
  }
  
  CreateTableStatement createScrambleMetaStoreTableStatement() {
    // create table
    CreateTableStatement query = new CreateTableStatement();
    query.setSchemaName(storeSchema);
    query.setTableName(getMetaStoreTableName());
    query.addColumnNameAndType(Pair.of(SCHEMA_COLUMN, "VARCHAR(100)"));
    query.addColumnNameAndType(Pair.of(TABLE_COLUMN, "VARCHAR(100)"));
    query.addColumnNameAndType(Pair.of(DATA_COLUMN, "TEXT"));
    query.setIfNotExists(true);
    return query;
  }
  
  public ScrambleMetaSet retrieve() {
    return null;
  }

}
