package edu.umich.verdict;

import java.sql.ResultSet;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.spark.sql.SQLContext;

import com.google.common.base.Optional;

import edu.umich.verdict.dbms.Dbms;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.query.Query;
import edu.umich.verdict.util.VerdictLogger;


public class VerdictContext {
	
	final protected VerdictConf conf;
	
	protected VerdictMeta meta;
	
	/*
	 *  DBMS fields
	 */
	private Dbms dbms;
	
	private Dbms metaDbms;		// contains persistent info of VerdictMeta
	
	
	// used for refreshing meta data.
	private long queryUid;
	
	final private int contextId;
	
	
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

	protected VerdictContext(VerdictConf conf) {
		this.conf = conf;
		this.contextId = ThreadLocalRandom.current().nextInt(0, 10000);
	}
	
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
		this.contextId = another.contextId;
	}
	
	/**
	 * Makes connections to the 'data' DBMS and 'meta' DBMS.
	 * @param conf
	 * @throws VerdictException
	 */
	public static VerdictContext from(VerdictConf conf) throws VerdictException {
		VerdictContext vc = new VerdictContext(conf);
		vc.setDbms(Dbms.from(vc, conf));
		vc.setMeta(new VerdictMeta(vc));		// this must be called after DB connection is created.
		
		if (conf.getDbmsSchema() != null) {
			vc.getMeta().refreshSampleInfo(conf.getDbmsSchema());
		}
		
		return vc;
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

}
