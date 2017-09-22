/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict;

import java.sql.ResultSet;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Optional;

import edu.umich.verdict.dbms.Dbms;
import edu.umich.verdict.dbms.DbmsJDBC;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.query.Query;
import edu.umich.verdict.util.VerdictLogger;


public class VerdictJDBCContext extends VerdictContext {
	
	VerdictMeta meta;
	
	/*
	 *  DBMS fields
	 */
	private Dbms dbms;
	
	private Dbms metaDbms;		// contains persistent info of VerdictMeta
	
	private ResultSet rs;
	
	
	// used for refreshing meta data.
	private long queryUid;
	
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
	
	/**
	 *  copy constructor
	 * @param conf
	 * @throws VerdictException
	 */
	public VerdictJDBCContext(VerdictJDBCContext another) throws VerdictException {
		super(another.conf, another.contextId);
		this.meta = another.meta;
		this.dbms = another.dbms;
		this.metaDbms = another.metaDbms;
		this.queryUid = another.queryUid;
//		((DbmsJDBC) this.dbms).createNewStatementWithoutClosing();
		this.rs = another.rs;
	}
	
	VerdictJDBCContext(VerdictConf conf) {
		super(conf);
	}
	
	/**
	 * Makes connections to the 'data' DBMS and 'meta' DBMS.
	 * @param conf
	 * @throws VerdictException
	 */
	public static VerdictJDBCContext from(VerdictConf conf) throws VerdictException {
		VerdictJDBCContext vc = new VerdictJDBCContext(conf);
		vc.setDbms(Dbms.from(vc, conf));
		vc.setMeta(new VerdictMeta(vc));		// this must be called after DB connection is created.
		
		if (conf.getDbmsSchema() != null) {
			vc.getMeta().refreshSampleInfo(conf.getDbmsSchema());
		}
		
		return vc;
	}
	
	public void execute(String sql) throws VerdictException {
		VerdictLogger.debug(this, "An input query:");
		VerdictLogger.debugPretty(this, sql, "  ");
		Query vq = Query.getInstance(this, sql);
		rs = vq.computeResultSet();
		VerdictLogger.debug(this, "The query execution finished.");
	}
	
//	public ResultSet executeQuery(String sql) throws VerdictException {
//		execute(sql);
//		return getResultSet();
//	}

	@Override
	public ResultSet getResultSet() {
		return rs;
	}

	@Override
	public DataFrame getDataFrame() {
		return null;
	}

	@Override
	public Dataset<Row> getDataset() {
		// TODO Auto-generated method stub
		return null;
	}

}
