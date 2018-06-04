package org.verdictdb.core.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MetaData {

    public class tableInfo {
        String schema;
        String tablename;
        String alias;
        List<String> colName = new ArrayList<>();

        public tableInfo(String schema, String tablename, String alias, List<String> colName) {
            this.schema = schema;
            this.tablename = tablename;
            this.alias = alias;
            this.colName = colName;
        }

    }

    private List<tableInfo> tables;

    public HashMap<String, String> columnAlias;

    public MetaData(List<tableInfo> tables) {
        this.tables = tables;
        for (tableInfo t : tables) {
            for (String c : t.colName) {
                columnAlias.put(c, t.alias);
            }
        }
    }
}
