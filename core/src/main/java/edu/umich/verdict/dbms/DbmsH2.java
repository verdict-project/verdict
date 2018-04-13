package edu.umich.verdict.dbms;

import com.google.common.base.Joiner;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.ResultSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 4/10/18.
 */
public class DbmsH2 extends DbmsJDBC{

    public DbmsH2(VerdictContext vc, String dbName, String host, String port, String schema, String user,
                  String password, String jdbcClassName) throws VerdictException {
        super(vc, dbName, host, port, schema, user, password, jdbcClassName);
    }


    @Override
    public String getQuoteString() {
        return "`";
    }

    protected String modOfRand(int mod) {
        return String.format("random() %% %d", mod);
    }

    @Override
    public String modOfHash(String col, int mod) {
        return String.format("abs(cast(cast(hash('SHA256',rpad(cast(%s%s%s as varchar(8)),8,'0'),1000) as varbinary(4)) as int)) %% %d", getQuoteString(), col, getQuoteString(), mod);
    }

    @Override
    public ResultSet getDatabaseNamesInResultSet() throws VerdictException {
        return executeJdbcQuery("show schemas");
    }

    @Override
    public void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException {
        StringBuilder sql = new StringBuilder(1000);
        sql.append(String.format("insert into %s values ", tableName));
        sql.append("(");
        String with = "'";
        sql.append(Joiner.on(", ").join(StringManipulations.quoteString(values, with)));
        sql.append(")");
        executeUpdate(sql.toString());
    }

    @Override
    public ResultSet describeTableInResultSet(TableUniqueName tableUniqueName) throws VerdictException {
        if (!tableUniqueName.getSchemaName().equalsIgnoreCase("information_schema")) {
            String sql = String.format(
                    "SELECT COLUMN_NAME, TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                            "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                    tableUniqueName.getSchemaName().toUpperCase(), // H2 stores them with upper case.
                    tableUniqueName.getTableName().toUpperCase());
            return executeJdbcQuery(sql);
        }
        return null;
    }

    @Override
    public void dropTable(TableUniqueName tableName, boolean check) throws VerdictException {
        Set<String> databases = vc.getMeta().getDatabases();

        // TODO: this is buggy when the database created while a query is executed.
        // it can happen during sample creations.
        // We force them to be a lowercase.
        if (check && !databases.contains(tableName.getSchemaName().toLowerCase())) {
            VerdictLogger.debug(this,
                    String.format(
                            "Database, %s, does not exists. Verdict doesn't bother to run a drop table statement.",
                            tableName.getSchemaName()));
            return;
        }

        // This check is useful for Spark 1.6, since it throws an error even though "if exists" is used
        // in the "drop table" statement.
        // We force them to be a lowercase.
        Set<String> tables = vc.getMeta().getTables(tableName.getDatabaseName());
        if (check && !tables.contains(tableName.getTableName().toLowerCase())) {
            VerdictLogger.debug(this, String.format(
                    "Table, %s, does not exists. Verdict doesn't bother to run a drop table statement.", tableName));
            return;
        }

        String sql = String.format("DROP TABLE IF EXISTS %s", tableName);
        VerdictLogger.debug(this, String.format("Drops table: %s", sql));
        executeUpdate(sql);
        vc.getMeta().refreshTables(tableName.getDatabaseName());
        VerdictLogger.debug(this, tableName + " has been dropped.");
    }

    @Override
    public void createMetaTablesInDBMS(TableUniqueName originalTableName, TableUniqueName sizeTableName, TableUniqueName nameTableName) throws VerdictException {
        VerdictLogger.debug(this, "Creates meta tables if not exist.");
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s", sizeTableName) + " (schemaname VARCHAR, "
                + " tablename VARCHAR, " + " samplesize BIGINT, " + " originaltablesize BIGINT)";
        executeUpdate(sql);

        sql = String.format("CREATE TABLE IF NOT EXISTS %s", nameTableName) + " (originalschemaname VARCHAR, "
                + " originaltablename VARCHAR, " + " sampleschemaaname VARCHAR, " + " sampletablename VARCHAR, "
                + " sampletype VARCHAR, " + " samplingratio DOUBLE, " + " columnnames VARCHAR)";
        executeUpdate(sql);

        VerdictLogger.debug(this, "Meta tables created.");
        vc.getMeta().refreshTables(sizeTableName.getDatabaseName());
    }

    @Override
    public String modOfHash(List<String> columns, int mod) {
        String concatStr = "";
        for (int i = 0; i < columns.size(); ++i) {
            String col = columns.get(i);
            String castStr = String.format("rpad(cast(rawtohex(%s%s%s) as varchar(8)),8,'0')", getQuoteString(), col, getQuoteString());
            if (i < columns.size() - 1) {
                castStr += ",";
            }
            concatStr += castStr;
        }
        return String.format("abs(cast(cast(hash('SHA256',concat_ws('%s', %s),1000) as varbinary(4)) as int)) %% %d", HASH_DELIM, concatStr, mod);
    }

    @Override
    public ResultSet getTablesInResultSet(String schema) throws VerdictException {
        return executeJdbcQuery("show tables from " + schema);
    }

    @Override
    protected String randomNumberExpression(SampleParam param) {
        String expr = "random()";
        return expr;
    }

    @Override
    protected String randomPartitionColumn() {
        int pcount = partitionCount();
        return String.format("mod(round(random()*%d), %d) AS %s", pcount, pcount, partitionColumnName());
    }
}
