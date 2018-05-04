package edu.umich.verdict.relation.expr;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.parser.VerdictSQLParser.Lateral_view_functionContext;

public class LateralFunc extends Expr {
    
    public enum LateralFuncName {
        EXPLODE, UNKNOWN
    }
    
    protected static Map<String, LateralFuncName> string2FunctionType =
            ImmutableMap.<String, LateralFuncName>builder()
            .put("EXPLODE", LateralFuncName.EXPLODE)
            .build();
    
    protected static Map<LateralFuncName, String> functionPattern =
            ImmutableMap.<LateralFuncName, String>builder()
            .put(LateralFuncName.EXPLODE, "explode(%s)")
            .put(LateralFuncName.UNKNOWN, "unknown(%s)")
            .build();
    
    private LateralFuncName funcname;
    
    private Expr expr;
    
//    private String tableAlias;
//    
//    private String columnAlias;
    
    public LateralFuncName getFuncName() {
        return funcname;
    }

    public Expr getExpression() {
        return expr;
    }
    
//    public String getTableAlias() {
//        return tableAlias;
//    }
//    
//    public String getColumnAlias() {
//        return columnAlias;
//    }

    public LateralFunc(LateralFuncName fname, Expr expr) {
        super(VerdictContext.dummyContext());
        this.funcname = fname;
        this.expr = expr;
//        this.tableAlias = tableAlias;
//        this.columnAlias = columnAlias;
    }
    
    public static LateralFunc from(final VerdictContext vc, Lateral_view_functionContext ctx) {
        String fname = ctx.function_name.getText().toUpperCase();
//        String tableAlias = (ctx.table_alias() == null)?  null : ctx.table_alias().getText();
//        String columnAlias = (ctx.column_alias() == null)?  null : ctx.column_alias().getText();
        
        if (string2FunctionType.containsKey(fname)) {
            return new LateralFunc(string2FunctionType.get(fname), Expr.from(vc, ctx.expression()));
        }
        return new LateralFunc(LateralFuncName.UNKNOWN, Expr.from(vc, ctx.expression()));
    }

    @Override
    public <T> T accept(ExprVisitor<T> v) {
        return v.call(this);
    }

    @Override
    public Expr withTableSubstituted(String newTab) {
        return new LateralFunc(funcname, expr.withTableSubstituted(newTab));
    }

    @Override
    public Expr withNewTablePrefix(String newPrefix) {
        return new LateralFunc(funcname, expr.withNewTablePrefix(newPrefix));
    }

    @Override
    public String toSql() {
        StringBuilder sql = new StringBuilder(50);
        sql.append(String.format(functionPattern.get(funcname), expr.toSql()));
//        if (tableAlias != null) {
//            sql.append(" " + tableAlias);
//        }
//        if (columnAlias != null) {
//            sql.append(" AS " + columnAlias);
//        }
        return sql.toString();
    }
    
    @Override
    public String toString() {
    	return toSql();
    }

    @Override
    public int hashCode() {
        return funcname.hashCode() + expr.hashCode();
    }

    @Override
    public boolean equals(Expr o) {
        if (o instanceof LateralFunc) {
            return getExpression().equals(((LateralFunc) o).getExpression())
                    && getFuncName().equals(((LateralFunc) o).getFuncName());
        }
        return false;
    }

}
