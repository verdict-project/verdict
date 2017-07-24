package edu.umich.verdict.dbms;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.util.VerdictLogger;

public class DbmsImpala extends DbmsJDBC {

	public DbmsImpala(VerdictContext vc, String dbName, String host, String port, String schema, String user,
			String password, String jdbcClassName) throws VerdictException {
		super(vc, dbName, host, port, schema, user, password, jdbcClassName);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void updateSampleNameEntryIntoDBMS(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		TableUniqueName tempTableName = createTempTableExlucdingNameEntry(param, metaNameTableName);
		insertSampleNameEntryIntoDBMS(param, tempTableName);
		moveTable(tempTableName, metaNameTableName);
	}

	protected TableUniqueName createTempTableExlucdingNameEntry(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		String metaSchema = param.sampleTableName().getSchemaName();
		TableUniqueName tempTableName = Relation.getTempTableName(vc, metaSchema);
		TableUniqueName originalTableName = param.originalTable;
		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s "
				+ "WHERE originalschemaname <> \"%s\" OR originaltablename <> \"%s\" OR sampletype <> \"%s\""
				+ "OR samplingratio <> %s OR columnnames <> \"%s\"",
				tempTableName, metaNameTableName, originalTableName.getSchemaName(), originalTableName.getTableName(),
				param.sampleType, samplingRatioToString(param.samplingRatio), columnNameListToString(param.columnNames)));
		return tempTableName;
	}
	
	@Override
	public String modOfHash(String col, int mod) {
		return String.format("abs(fnv_hash(cast(%s AS STRING))) %% %d", col, mod);
	}
	
	@Override
	public String modOfRand(int mod) {
		return String.format("abs(rand(unix_timestamp())) %% %d", mod);
	}

	@Override
	protected String randomNumberExpression(SampleParam param) {
		Map<String, String> col2types = vc.getMeta().getColumn2Types(param.originalTable);
		Set<String> hashCols = new HashSet<String>();
		int precision = 3;
		int modValue = (int) Math.pow(10, precision);
		
		for (Map.Entry<String, String> col2type : col2types.entrySet()) {
			String col = col2type.getKey();
			String type = col2type.getValue();
			if (type.toLowerCase().contains("char") || type.toLowerCase().contains("str")) {
				hashCols.add(String.format("fnv_hash((case when %s is null then cast(unix_timestamp() as string) else %s end))", col, col));
			} else {
				hashCols.add(String.format("fnv_hash((case when %s is null then unix_timestamp() else %s end))", col, col));
			}
		}
		String expr = "abs(fnv_hash("
		                     + Joiner.on(" + ").join(hashCols) 
		                     + String.format(" + unix_timestamp())) %% %d / %d", modValue, modValue);
		return expr;
	}

	protected String randomPartitionColumn() {
		int pcount = partitionCount();
		return String.format("round(rand(unix_timestamp())*%d) %% %d AS %s", pcount, pcount, partitionColumnName());
	}

}
