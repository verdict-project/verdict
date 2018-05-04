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

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.parser.VerdictSQLParser.Table_nameContext;

// Currently not used.
public class TableNameExpr extends Expr {
    
    private String schema = null;
    
    private String table = null;

    /**
     * 
     * @param vc
     * @param schema May be null
     * @param table Must not be null
     */
    public TableNameExpr(VerdictContext vc, String schema, String table) {
        super(vc);
        assert(table != null);
        setSchema(schema);
        setTable(table);
    }
    
    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = (schema == null)? null : schema.toLowerCase();
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = (table == null)? null : table.toLowerCase();
    }

    public static TableNameExpr from(VerdictContext vc, Table_nameContext ctx) {
        String schema = null;
        String table = null;
        if (ctx.schema != null) {
            schema = ctx.schema.getText();
        }
        if (ctx.table != null) {
            table = ctx.table.getText();
        }
        return new TableNameExpr (vc, schema, table);
    }

    @Override
    public <T> T accept(ExprVisitor<T> v) {
        return v.call(this);
    }

    @Override
    public Expr withTableSubstituted(String newTab) {
        return this;
    }

    @Override
    public Expr withNewTablePrefix(String newPrefix) {
        return this;
    }

    @Override
    public String toSql() {
        return toString();
    }
    
    @Override
    public String toString() {
        if (schema == null) {
            return table;
        } else {
            return schema + "." + table;
        }
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Expr o) {
        if (o instanceof TableNameExpr) {
            return true;
        }
        return false;
    }

}
