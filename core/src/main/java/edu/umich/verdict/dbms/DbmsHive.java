package edu.umich.verdict.dbms;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StringManipulations;

public class DbmsHive extends DbmsJDBC {

    public DbmsHive(VerdictContext vc, String dbName, String host, String port, String schema, String user,
            String password, String jdbcClassName) throws VerdictException {
        super(vc, dbName, host, port, schema, user, password, jdbcClassName);
    }

    @Override
    public void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException {
        StringBuilder sql = new StringBuilder(1000);
        sql.append(String.format("insert into table %s select * from (select ", tableName));
        String with = "'";
        sql.append(Joiner.on(", ").join(StringManipulations.quoteString(values, with)));
        sql.append(") s");
        executeUpdate(sql.toString());
    }

    @Override
    public String getQuoteString() {
        return "`";
    }

    @Override
    protected String modOfRand(int mod) {
        return String.format("abs(rand(unix_timestamp())) %% %d", mod);
    }

    @Override
    public String modOfHash(String col, int mod) {
        return String.format("pmod(crc32(cast(%s as string)),%d)", col, mod);
    }

    @Override
    protected String randomNumberExpression(SampleParam param) {
        String expr = "rand(unix_timestamp())";
        return expr;
    }

    @Override
    protected String randomPartitionColumn() {
        int pcount = partitionCount();
        return String.format("pmod(round(rand(unix_timestamp())*%d), %d) AS %s", pcount, pcount, partitionColumnName());
    }

	@Override
	public Dataset<Row> getDataset() {
		// TODO Auto-generated method stub
		return null;
	}

}
