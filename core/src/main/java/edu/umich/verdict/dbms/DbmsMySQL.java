package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.SingleRelation;
import edu.umich.verdict.util.VerdictLogger;

public class DbmsMySQL extends DbmsJDBC {

	public DbmsMySQL(VerdictContext vc, String dbName, String host, String port, String schema, String user,
			String password, String jdbcClassName) throws VerdictException {
		super(vc, dbName, host, port, schema, user, password, jdbcClassName);
	}
	
//	public boolean doesMetaTablesExist(String schemaName) throws VerdictException {
//		String[] types = {"TABLE"};
//		try {
//			ResultSet rs = conn.getMetaData()
//							   .getTables(vc.getMeta().metaCatalogForDataCatalog(schemaName),
//									   	  null,
//									      vc.getMeta().getMetaNameTableForOriginalSchema(currentSchema.get()).getTableName(),
//									      types);
//			if (!rs.next()) return false;
//			else return true;
//		} catch (SQLException e) {
//			throw new VerdictException(e);
//		}
//	}

	/**
	 * Creates a sample table without dropping an old table.
	 * @param originalTableName
	 * @param sampleRatio
	 * @throws VerdictException
	 */
	protected void justCreateUniformRandomSampleTableOf(SampleParam param) throws VerdictException {
		String sql = String.format("CREATE TABLE %s AS ", param.sampleTableName()) + 
				 SingleRelation.from(vc, param.originalTable)
			 	 .where("rand() <= " + param.samplingRatio)
				 .select("*, round(rand()*100)%100 AS " + partitionColumnName()).toSql();
		
		VerdictLogger.debug(this, String.format("Creates a table: %s", sql));
		this.executeUpdate(sql);
		VerdictLogger.debug(this, "Done.");
	}
	
	public String modOfHash(String col, int mod) {
		return String.format("mod(cast(conv(substr(md5(%s),17,32),16,10) as unsigned), %d)", col, mod);
	}
	
	protected void justCreateUniverseSampleTableOf(SampleParam param) throws VerdictException {
		TableUniqueName sampleTableName = param.sampleTableName();
		String sql = String.format("CREATE TABLE %s AS ", sampleTableName) + 
				 	 SingleRelation.from(vc, param.originalTable)
				 	 .where(modOfHash(param.columnNames.get(0), 10000)
				 			 + String.format(" <= %.4f", param.samplingRatio*10000))
				 	 .select("*, round(rand()*100)%100 AS " + partitionColumnName()).toSql();
		
		VerdictLogger.debug(this, String.format("Creates a table: %s", sql));
		executeUpdate(sql);
		VerdictLogger.debug(this, "Done.");
	}

	@Override
	protected void justCreateStratifiedSampleTableof(SampleParam param) throws VerdictException {
		VerdictLogger.warn(this, "Stratified samples are not implemented for MySQL.");
	}

	@Override
	public List<String> getTables(String schema) throws VerdictException {
		List<String> tables = new ArrayList<String>();
		try {
			ResultSet rs = executeJdbcQuery("show tables in " + schema);
			while (rs.next()) {
				String table = rs.getString(1);
				tables.add(table);
			}
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
		return tables;
	}

	@Override
	public List<String> getColumns(TableUniqueName table) throws VerdictException {
		List<String> columns = new ArrayList<String>();
		try {
			ResultSet rs = executeJdbcQuery("describe " + table);
			while (rs.next()) {
				String column = rs.getString(1);
				columns.add(column);
			}
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
		return columns;
	}
}
