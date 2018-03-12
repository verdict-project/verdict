/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.dbms;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;
//import org.apache.spark.sql.DataFrame;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StackTraceReader;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public abstract class DbmsJDBC extends Dbms {

    protected final Connection conn;

    protected Statement stmt; // created Statements must be registered here.

    public Connection getDbmsConnection() {
        return conn;
    }

    protected List<Statement> allOpenStatements;

    public ResultSet getDatabaseNamesInResultSet() throws VerdictException {
        return executeJdbcQuery("show databases");
    }

    protected ResultSet rs;

    /**
     * Copy constructor for not sharing the underlying statement.
     *
     * @param another
     */
    public DbmsJDBC(Dbms another) {
        super(another);
        if (another instanceof DbmsJDBC) {
            conn = ((DbmsJDBC) another).conn;
        } else {
            conn = null;
        }
        stmt = null;
        allOpenStatements = new ArrayList<Statement>();
    }

    protected DbmsJDBC(VerdictContext vc, String dbName, String host, String port, String schema, String user,
            String password, String jdbcClassName) throws VerdictException {
        super(vc, dbName);
        currentSchema = Optional.fromNullable(schema);
        String url = composeUrl(dbName, host, port, schema, user, password);
        conn = makeDbmsConnection(url, jdbcClassName);
        stmt = null;
        allOpenStatements = new ArrayList<Statement>();
    }

    public ResultSet describeTableInResultSet(TableUniqueName tableUniqueName) throws VerdictException {
        return executeJdbcQuery(String.format("describe %s", tableUniqueName));
    }

    @Override
    public Set<String> getDatabases() throws VerdictException {
        Set<String> databases = new HashSet<String>();
        try {
            ResultSet rs = getDatabaseNamesInResultSet();
            while (rs.next()) {
                databases.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new VerdictException(StackTraceReader.stackTrace2String(e));
        }
        return databases;
    }

    public ResultSet getTablesInResultSet(String schema) throws VerdictException {
        return executeJdbcQuery("show tables in " + schema);
    }

    @Override
    public List<String> getTables(String schema) throws VerdictException {
        List<String> tables = new ArrayList<String>();
        try {
            ResultSet rs = getTablesInResultSet(schema);
            while (rs.next()) {
                String table = rs.getString(1);
                tables.add(table);
            }
        } catch (SQLException e) {
            VerdictLogger.error(this, "Failed to access the database: " + schema);
            throw new VerdictException(e);
        }
        return tables;
    }

    @Override
    public Map<String, String> getColumns(TableUniqueName table) throws VerdictException {
        Map<String, String> col2type = new LinkedHashMap<String, String>();
        try {
            ResultSet rs = describeTableInResultSet(table);
            while (rs.next()) {
                String column = rs.getString(1);
                if (!column.isEmpty()) {
                    if (column.substring(0,1).equals("#")) {
                        break;
                    }
                    String type = rs.getString(2);
                    col2type.put(column, type);
                }
            }
        } catch (SQLException e) {
            throw new VerdictException(e);
        }
        return col2type;
    }

    String composeUrl(String dbms, String host, String port, String schema, String user, String password)
            throws VerdictException {
        StringBuilder url = new StringBuilder();
        url.append(String.format("jdbc:%s://%s:%s", dbms, host, port));

        if (schema != null) {
            url.append(String.format("/%s", schema));
        }

        if (!vc.getConf().ignoreUserCredentials() && user != null && user.length() != 0) {
            url.append(";");
            url.append(String.format("user=%s", user));
        }
        if (!vc.getConf().ignoreUserCredentials() && password != null && password.length() != 0) {
            url.append(";");
            url.append(String.format("password=%s", password));
        }

        // set kerberos option if set
        if (vc.getConf().isJdbcKerberosSet()) {
            String value = vc.getConf().getJdbcKerberos();
            Pattern princPattern = Pattern.compile("(?<service>.*)/(?<host>.*)@(?<realm>.*)");
            Matcher princMatcher = princPattern.matcher(value);

            if (princMatcher.find()) {
                String service = princMatcher.group("service");
                String krbRealm = princMatcher.group("realm");
                String krbHost = princMatcher.group("host");

//                url.append(String.format(";AuthMech=%s;KrbRealm=%s;KrbHostFQDN=%s;KrbServiceName=%s;KrbAuthType=%s",
//                        "1", krbRealm, krbHost, service, "2"));
                url.append(String.format(";AuthMech=%s;KrbRealm=%s;KrbHostFQDN=%s;KrbServiceName=%s",
                        "1", krbRealm, krbHost, service));
            } else {
                VerdictLogger.error("Error: principal \"" + value + "\" could not be parsed.\n"
                        + "Make sure the principal is in the form service/host@REALM");
            }

            url.append(String.format(";principal=%s", value));
        }

        // pass other configuration options.
        for (Map.Entry<String, String> pair : vc.getConf().getConfigs().entrySet()) {
            String key = pair.getKey();
            String value = pair.getValue();

            if (key.startsWith("verdict") || key.equals("user") || key.equals("password")) {
                continue;
            }

            url.append(String.format(";%s=%s", key, value));
        }

        return url.toString();
    }

    protected Connection makeDbmsConnection(String url, String className) throws VerdictException {
        try {
            Class.forName(className);
            String passMasked = url.replaceAll("(;password)=([^;]+)", "$1=masked").replaceAll("(;PWD)=([^;]+)",
                    "$1=masked");
            ;
            VerdictLogger.info(this, "JDBC connection string (password masked): " + passMasked);
            Connection conn = DriverManager.getConnection(url);
            return conn;
        } catch (ClassNotFoundException | SQLException e) {
            throw new VerdictException(e);
        }
    }

    public long getTableSize(TableUniqueName tableName) throws VerdictException {
        ResultSet rs;
        long cnt = 0;
        try {
            String sql = String.format("SELECT COUNT(*) FROM %s", tableName);
            rs = executeJdbcQuery(sql);
            while (rs.next()) {
                cnt = rs.getLong(1);
            }
            rs.close();
        } catch (SQLException e) {
            throw new VerdictException(StackTraceReader.stackTrace2String(e));
        }
        return cnt;
    }

    public boolean execute(String sql) throws VerdictException {
        // createStatementIfNotExists();
        VerdictLogger.debug(this, "About to run: " + sql);
        createStatement();
        VerdictLogger.debug(this, "A new statement id: " + System.identityHashCode(stmt));
        boolean hasResult = false;
        try {
            hasResult = stmt.execute(sql);
            if (hasResult) {
                rs = stmt.getResultSet();
            } else {
                rs = null;
            }
        } catch (SQLException e) {
            throw new VerdictException(e);
        }
        return hasResult;
    }

    public void executeUpdate(String sql) throws VerdictException {
        // createStatementIfNotExists();
        VerdictLogger.debug(this, "About to run: " + sql);
        createStatement();
        VerdictLogger.debug(this, "A new statement id: " + System.identityHashCode(stmt));
        try {
            stmt.executeUpdate(sql);
            rs = null;
        } catch (SQLException e) {
            throw new VerdictException(e);
        }
    }

    public Statement createStatement() throws VerdictException {
        try {
            stmt = conn.createStatement();
            allOpenStatements.add(stmt);
        } catch (SQLException e) {
            throw new VerdictException(e);
        }
        return stmt;
    }

    public Statement getStatement() {
        return stmt;
    }

//    public Statement createNewStatementWithoutClosing() throws VerdictException {
//        try {
//            stmt = conn.createStatement();
//        } catch (SQLException e) {
//            throw new VerdictException(e);
//        }
//        return stmt;
//    }

    public void closeStatement() throws VerdictException {
        try {
            for (Statement s : allOpenStatements) {
                if (s != null && !s.isClosed()) {
                    s.close();
                }
            }
            allOpenStatements.clear();
            stmt = null;
        } catch (SQLException e) {
            throw new VerdictException(e);
        }
    }

    @Override
    public ResultSet getResultSet() {
        return rs;
    }

//    @Override
//    public DataFrame getDataFrame() {
//        return null;
//    }

    @Override
    public void deleteEntry(TableUniqueName tableName, List<Pair<String, String>> colAndValues)
            throws VerdictException {
        StringBuilder sql = new StringBuilder(1000);
        sql.append(String.format("delete from %s ", tableName));
        if (colAndValues.size() > 0) {
            sql.append("where ");
            List<String> conds = new ArrayList<String>();
            for (Pair<String, String> p : colAndValues) {
                conds.add(String.format("%s = %s", p.getLeft(), p.getRight()));
            }
            sql.append(Joiner.on(" AND ").join(conds));
        }
        executeUpdate(sql.toString());
    }

    @Override
    public void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException {
        StringBuilder sql = new StringBuilder(1000);
        sql.append(String.format("insert into table %s values ", tableName));
        sql.append("(");
        String with = "'";
        sql.append(Joiner.on(", ").join(StringManipulations.quoteString(values, with)));
        sql.append(")");
        executeUpdate(sql.toString());
    }

    @Override
    public boolean isJDBC() {
        return true;
    }

    public void close() throws VerdictException {
        try {
            closeStatement();
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            throw new VerdictException(e);
        }
    }

}
