package edu.umich.verdict.dbms;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.ResultSet;
import java.util.List;

/**
 * Created by Dong Young Yoon on 4/10/18.
 */
public class DbmsH2 extends DbmsJDBC{

    public DbmsH2(VerdictContext vc, String dbName, String host, String port, String schema, String user,
                  String password, String jdbcClassName) throws VerdictException {
        super(vc, dbName, host, port, schema, user, password, jdbcClassName);
    }

    @Override
    protected String modOfRand(int mod) {
        return String.format("random() %% %d", mod);
    }

    @Override
    public String modOfHash(String col, int mod) {
        return String.format("abs(cast(cast(hash('SHA256',rpad(cast(%s%s%s as varchar(8)),8,'0'),1000) as varbinary(4)) as int)) %% %d", getQuoteString(), col, getQuoteString(), mod);
    }

    @Override
    public ResultSet describeTableInResultSet(TableUniqueName tableUniqueName) throws VerdictException {
        if (!tableUniqueName.getSchemaName().equalsIgnoreCase("information_schema")) {
            String sql = String.format(
                    "SELECT COLUMN_NAME, TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                            "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                    tableUniqueName.getSchemaName(), tableUniqueName.getTableName());
            return executeJdbcQuery(sql);
        }
        return null;
    }

    @Override
    public String modOfHash(List<String> columns, int mod) {
        String concatStr = "";
        for (int i = 0; i < columns.size(); ++i) {
            String col = columns.get(i);
            String castStr = String.format("rpad(cast(%s%s%s as varchar(8)),8,'0')", getQuoteString(), col, getQuoteString());
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

    @Override
    public Dataset<Row> getDataset() {
        // TODO Auto-generated method stub
        return null;
    }
}
