package edu.umich.verdict;

import java.sql.ResultSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.spark.sql.DataFrame;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import edu.umich.verdict.dbms.Dbms;
import edu.umich.verdict.exceptions.VerdictException;

public abstract class VerdictContext {

	final protected VerdictConf conf;
	
	protected VerdictMeta meta;
	
	final static protected Set<String> JDBC_DBMS = Sets.newHashSet("mysql", "impala", "hive", "hive2");
	
	/*
	 *  DBMS fields
	 */
	private Dbms dbms;
	
	private Dbms metaDbms;		// contains persistent info of VerdictMeta
	
	
	// used for refreshing meta data.
	private long queryUid;
	
	final protected int contextId;
	
	
	public Dbms getDbms() {
		return dbms;
	}

	public void setDbms(Dbms dbms) {
		this.dbms = dbms;
		this.metaDbms = dbms;
	}

	public int getContextId() {
		return contextId;
	}

	public long getQid() {
		return queryUid;
	}

	public VerdictMeta getMeta() {
		return meta;
	}
	
	public void setMeta(VerdictMeta meta) {
		this.meta = meta;
	}

	public String getDefaultSchema() {
		return conf.getDbmsSchema();
	}

	public VerdictConf getConf() {
		return conf;
	}

	public Dbms getMetaDbms() {
		return metaDbms;
	}

	public Optional<String> getCurrentSchema() {
		return dbms.getCurrentSchema();
	}

	public void destroy() throws VerdictException {
		dbms.close();
	}

	public long getCurrentQid() {
		return queryUid;
	}

	public void incrementQid() {
		queryUid += 1;
	}
	
	protected VerdictContext(VerdictConf conf, int contextId) {
		this.conf = conf;
		this.contextId = contextId;
	}
	
	protected VerdictContext(VerdictConf conf) {
		this(conf, ThreadLocalRandom.current().nextInt(0, 10000));
	}
	
	private static VerdictContext dummyContext = null;
	
	/**
	 * Singleton dummy VerdictContext. Used only by FuncExpr for setting inherited Expr's VerdictContext field.
	 * @return
	 */
	public static VerdictContext dummyContext() {
	    if (dummyContext != null) return dummyContext;
	    
		VerdictConf conf = new VerdictConf(false);
		conf.setDbms("dummy");
		try {
			dummyContext = VerdictJDBCContext.from(conf);
			return dummyContext;
		} catch (VerdictException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public abstract void execute(String sql) throws VerdictException;
	
	public abstract ResultSet getResultSet();
	
	public abstract DataFrame getDataFrame();
	
	public ResultSet executeJdbcQuery(String sql) throws VerdictException {
		execute(sql);
		ResultSet rs = getResultSet();
		return rs;
	}
	
	public DataFrame executeSparkQuery(String sql) throws VerdictException {
		execute(sql);
		DataFrame df = getDataFrame();
		return df;
	}
	
	public DataFrame sql(String sql) throws VerdictException {
		DataFrame df = executeSparkQuery(sql);
		return df;
	}

}
