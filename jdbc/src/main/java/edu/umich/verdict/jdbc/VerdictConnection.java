package edu.umich.verdict.jdbc;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.dbms.DbmsJDBC;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StackTraceReader;
import edu.umich.verdict.util.VerdictLogger;


public class VerdictConnection implements Connection {
    //    private final DbConnector connector;
    //    private final Connection innerConnection;
    private final VerdictConf conf;
    private VerdictJDBCContext vc;
    private boolean isOpen = true;

    private Connection getDbmsConnection() {
        return ((DbmsJDBC) vc.getDbms()).getDbmsConnection();
    }

    //    public void setLogLevel(String level) {
    //    	vc.setLogLevel(level);
    //    }

    /**
     * Option priorities
     * 1. Explicitly passed key-values in {@code url}
     * 2. key-values stored in {@code info}
     * 3. default properties (automatically handled by {@link edu.umich.verdict.VerdictConf VerdictConf}.
     * @param url Should be in the format "jdbc:verdict:dbms://host:port[/schema][?config=/path/to/conf]"
     * @param info
     * @throws SQLException
     */
    public VerdictConnection(String url, Properties info) throws SQLException {
        try{
            // set properties from the config file
            if (info.contains("configfile")) {
                conf = new VerdictConf(info.getProperty("configfile"));
            } else {
                conf = new VerdictConf();		// by default, this loads configs from the file
            }

            // set properties from the passed connection string.
            Pattern inlineOptions = Pattern.compile("(?<key>[a-zA-Z0-9_\\.]+)=[\"']?(?<value>[a-zA-Z0-9@_/\\.\\\\:-]+)[\"']?");
            Matcher inlineMatcher = inlineOptions.matcher(url);
            while (inlineMatcher.find()) {
                info.setProperty(inlineMatcher.group("key"), inlineMatcher.group("value"));
            }
            conf.setProperties(info);

            for (Map.Entry<Object, Object> e : info.entrySet()) {
                VerdictLogger.debug(this, String.format("connection properties: %s = %s", e.getKey(), e.getValue()));
            }

            // set properties from the url string.    	
            Pattern urlOptions = Pattern.compile("^jdbc:verdict:(?<dbms>\\w+)://(?<host>[\\.a-zA-Z0-9\\-]+)(?::(?<port>\\d+))?(?:/(?<schema>\\w+))?"
                    + "(\\?(?<extras>.*))?");
            Matcher urlMatcher = urlOptions.matcher(url);
            if (!urlMatcher.find())
                throw new SQLException("Invalid URL.");

            conf.setDbms(urlMatcher.group("dbms"));
            conf.setHost(urlMatcher.group("host"));
            if (urlMatcher.group("port") != null) {
                conf.setPort(urlMatcher.group("port"));
            } else {
                conf.setPort(conf.getDefaultPort());		// assume config file includes it.
            }
            if (urlMatcher.group("schema") != null) {
                conf.setDbmsSchema(urlMatcher.group("schema"));
            }

            String extras = urlMatcher.group("extras");
            if (extras != null) {
                Matcher extraMatcher = inlineOptions.matcher(extras);

                while (extraMatcher.find()) {
                    conf.set(extraMatcher.group("key"), extraMatcher.group("value"));
                }
            }

            this.vc = VerdictJDBCContext.from(conf); 

        } catch (VerdictException e) {
            throw new SQLException(StackTraceReader.stackTrace2String(e));
        }
    }


    @Override
    public Statement createStatement() throws SQLException {
        // we create a copy of VerdictContext so that the underlying statement is not shared.
        return new VerdictStatement(this, vc);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new VerdictPreparedStatement(sql, vc, this);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support callable statements");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        getDbmsConnection().setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return getDbmsConnection().getAutoCommit(); 
    }

    @Override
    public void commit() throws SQLException {
        getDbmsConnection().commit();
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support updates (thus nor commits)");
    }

    @Override
    public void close() throws SQLException {
        try {
            vc.destroy();
        } catch (VerdictException e) {
            throw new SQLException(StackTraceReader.stackTrace2String(e));
        }
        isOpen = false;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return !isOpen;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return getDbmsConnection().getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        getDbmsConnection().setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return getDbmsConnection().isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        getDbmsConnection().setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return getDbmsConnection().getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        getDbmsConnection().setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return getDbmsConnection().getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return getDbmsConnection().getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        getDbmsConnection().clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new VerdictStatement(this, vc);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support prepared statements");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support callable statements");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return getDbmsConnection().getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        getDbmsConnection().setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        getDbmsConnection().setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return getDbmsConnection().getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return getDbmsConnection().setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return getDbmsConnection().setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        getDbmsConnection().rollback();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        getDbmsConnection().releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new VerdictStatement(this, vc);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support prepared statement");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support callable statement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support prepared statement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support prepared statement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support prepared statement");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support clob type");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support blob type");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support nclob type");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support xml type");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return isOpen;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        getDbmsConnection().setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        getDbmsConnection().setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return getDbmsConnection().getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return getDbmsConnection().getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support Array type");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support Struct type");
    }

    @Override
    public void setSchema(String schema) {
        vc.getConf().setDbmsSchema(schema);
    }

    @Override
    public String getSchema() {
        return vc.getConf().getDbmsSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        getDbmsConnection().abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        getDbmsConnection().setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return getDbmsConnection().getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support wrap");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support wrap");
    }
}