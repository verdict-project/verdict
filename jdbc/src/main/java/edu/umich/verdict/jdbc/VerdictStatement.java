package edu.umich.verdict.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;

import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.dbms.DbmsJDBC;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StackTraceReader;
import edu.umich.verdict.util.VerdictLogger;

public class VerdictStatement implements Statement {

    private final VerdictConnection connection;
    
    private Statement stmt;

    private final ArrayList<String> batch = new ArrayList<String>();	// TODO: support batch operations.
    
    private final VerdictJDBCContext vc;

    private ResultSet answer;


    public VerdictStatement(VerdictConnection connection, VerdictJDBCContext vc) throws SQLException {
        this.connection = connection;
        try {
            // a new verdict context does not share the underlying statement.
            this.vc = vc;
//            ((DbmsJDBC) vc.getDbms()).createNewStatementWithoutClosing();
            this.stmt = ((DbmsJDBC) vc.getDbms()).createStatement();
        } catch (VerdictException e) {
            throw new SQLException(StackTraceReader.stackTrace2String(e));
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        VerdictLogger.debug(this, String.format("executeQuery() called with: %s", sql));
        execute(sql);
        return getResultSet();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        VerdictLogger.debug(this, String.format("executeUpdate() called with: %s", sql));
        execute(sql);
        return 0;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        VerdictLogger.debug(this, String.format("execute() called with: %s", sql));
        try {
            answer = vc.executeJdbcQuery(sql);
//            this.stmt = ((DbmsJDBC) vc.getDbms()).getStatement();
        } catch (VerdictException e) {
            throw new SQLException(StackTraceReader.stackTrace2String(e));
        }
        return (answer != null)? true : false;
    }

    @Override
    public void close() throws SQLException {
        try {
            ((DbmsJDBC) vc.getDbms()).closeStatement();
        } catch (VerdictException e) {
            new SQLException(StackTraceReader.stackTrace2String(e));
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return stmt.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        stmt.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return stmt.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        stmt.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        stmt.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return stmt.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        stmt.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        stmt.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return stmt.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        stmt.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        stmt.setCursorName(name);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return answer;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return stmt.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        //    	VerdictLogger.warn(this, "getMoreResults() is not supported");
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        stmt.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return stmt.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        stmt.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return stmt.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return stmt.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return stmt.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLException("Verdict currently doesn't support batch processing.");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLException("Verdict currently doesn't support batch processing.");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLException("Verdict currently doesn't support batch processing.");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        //    	VerdictLogger.warn(this, "getMoreResults() is not supported");
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLException("Verdict currently doesn't support generatedKeys.");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Verdict currently doesn't support autoGeneratedKeys.");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Verdict currently doesn't support this function.");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Verdict currently doesn't support this function.");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Verdict currently doesn't support this function.");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Verdict currently doesn't support this function.");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Verdict currently doesn't support this function.");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return stmt.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return (stmt == null)? true : stmt.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        stmt.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return stmt.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        stmt.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return stmt.isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support wrap");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Verdict doesn't support wrap");
    }
}
