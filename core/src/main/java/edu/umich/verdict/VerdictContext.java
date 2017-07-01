package edu.umich.verdict;

import java.sql.ResultSet;

import com.google.common.base.Optional;

import edu.umich.verdict.dbms.Dbms;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.query.Query;
import edu.umich.verdict.util.VerdictLogger;


public class VerdictContext {
	
	final private VerdictConf conf;
	
	final private VerdictMeta meta;
	
	/*
	 *  DBMS fields
	 */
	private Dbms dbms;
	
	private Dbms metaDbms;		// contains persistent info of VerdictMeta
	
	
	// used for refreshing meta data.
	private long queryUid;
	
	
	/* 
	 * predefined constants
	 */
	
	// the column name for the sampling probabilities in a stratified sample. 
	private final String ST_SAMPLING_PROB_COL = "verdict_sampling_prob";
	
	/**
	 *  copy constructor
	 * @param conf
	 * @throws VerdictException
	 */
	public VerdictContext(VerdictContext another) throws VerdictException {
		this.conf = another.conf;
		this.meta = another.meta;
		this.dbms = another.dbms;
		this.metaDbms = another.metaDbms;
		this.queryUid = another.queryUid;
		this.dbms.createNewStatementWithoutClosing();
	}
	
	/**
	 * Makes connections to the 'data' DBMS and 'meta' DBMS.
	 * @param conf
	 * @throws VerdictException
	 */
	public VerdictContext(VerdictConf conf) throws VerdictException {
		this.conf = conf;
		
		dbms = Dbms.getInstance(this,
								conf.getDbms(),
								conf.getHost(),
								conf.getPort(),
								conf.getDbmsSchema(),
								(conf.getBoolean("no_user_password"))? "" : conf.getUser(),
								(conf.getBoolean("no_user_password"))? "" : conf.getPassword(),
								conf.get(conf.getDbms() + ".jdbc_class_name"));
		VerdictLogger.info( 
				(conf.getDbmsSchema() != null) ?
						String.format("Connected to database: %s//%s:%s/%s",
								conf.getDbms(), conf.getHost(), conf.getPort(), conf.getDbmsSchema())
						: String.format("Connected to database: %s//%s:%s",
								conf.getDbms(), conf.getHost(), conf.getPort()));
		
		metaDbms = dbms;
		meta = new VerdictMeta(this);		// this must be called after DB connection is created.
		
		if (conf.getDbmsSchema() != null) {
			meta.refreshSampleInfo(conf.getDbmsSchema());
		}
	}
	
	public ResultSet executeQuery(String sql) throws VerdictException {
		VerdictLogger.debug(this, "An input query:");
		VerdictLogger.debugPretty(this, sql, "  ");
		Query vq = Query.getInstance(this, sql);
		ResultSet rs = vq.compute();
		VerdictLogger.debug(this, "The query execution finished.");
		return rs;
	}
	
	public void incrementQid() {
		queryUid += 1;
	}
	
	public long getQid() {
		return queryUid;
	}
	
	public VerdictMeta getMeta() {
		return meta;
	}
	
	public String getDefaultSchema() {
		return conf.getDbmsSchema();
	}
	
	public VerdictConf getConf() {
		return conf;
	}
	
	public Dbms getDbms() {
		return dbms;
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
	
	public String samplingProbColName() {
		return ST_SAMPLING_PROB_COL;
	}
}
