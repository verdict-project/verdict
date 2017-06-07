package edu.umich.verdict;

import java.sql.ResultSet;

import com.google.common.base.Optional;

import edu.umich.verdict.dbms.Dbms;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.query.Query;
import edu.umich.verdict.util.VerdictLogger;


public class VerdictContext {
	
	private VerdictConf conf;
	private VerdictMeta meta;
	
	// DBMS fields
	private Dbms dbms;
	private Dbms metaDbms;		// contains persistent info of VerdictMeta
	
	// used for refreshing meta data.
	private long queryUid;
	
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
		this.dbms.createNewStatementWithoutClosing();
		this.queryUid = 0;
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
								conf.getUser(),
								conf.getPassword(),
								conf.get(conf.getDbms() + ".jdbc_class_name"));
		VerdictLogger.info( 
				(conf.getDbmsSchema() != null) ?
						String.format("Connected to database: %s//%s:%s/%s",
								conf.getDbms(), conf.getHost(), conf.getPort(), conf.getDbmsSchema())
						: String.format("Connected to database: %s//%s:%s",
								conf.getDbms(), conf.getHost(), conf.getPort()));
		
		metaDbms = dbms;

//		metaDbms = Dbms.getInstance(this,
//									conf.getMetaDbms(),
//									conf.getMetaHost(),
//									conf.getMetaPort(),
//									conf.getMetaDbmsSchema(),
//									conf.getMetaUser(),
//									conf.getMetaPassword(),
//									conf.get(conf.getMetaDbms() + ".jdbc_class_name"));
//		VerdictLogger.info(this, String.format("Connected to meta DB: %s//%s/%s",
//				conf.getMetaDbms(), conf.getMetaHost(), conf.getMetaDbmsSchema()));
		
		meta = new VerdictMeta(this);		// this must be called after DB connection is created.
	}
	
	public ResultSet executeQuery(String sql) throws VerdictException {
		queryUid++;
		VerdictLogger.debug(this, "A query execution starts:");
		VerdictLogger.debugPretty(this, sql, "  ");
		Query vq = new Query(sql, this);
		ResultSet rs = vq.compute();
		VerdictLogger.debug(this, "A query execution finished");
		return rs;
	}
	
//	public void executeUpdate(String sql) throws VerdictException {
//		VerdictLogger.info(this, "A query execution starts: " + sql);
//		VerdictQuery vq = new VerdictQuery(sql, this);
//		vq.computeUpdate();
//		VerdictLogger.info(this, "A query execution finished");
//	}
	
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
	
//	public void setLogLevel(String level) {
//		VerdictLogger.setLogLevel(level);
//	}
	
}
