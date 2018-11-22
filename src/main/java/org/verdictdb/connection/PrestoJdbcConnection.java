package org.verdictdb.connection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.SqlSyntax;

import com.facebook.presto.jdbc.PrestoStatement;
import com.facebook.presto.jdbc.QueryStats;

import me.tongfei.progressbar.ProgressBar;

public class PrestoJdbcConnection extends JdbcConnection {

  public PrestoJdbcConnection(Connection conn, SqlSyntax syntax) {
    super(conn, syntax);
  }
  
  public void ensureCatalogSet() throws VerdictDBDbmsException {
    String catalog = null;
    try {
      catalog = getConnection().getCatalog();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    if (catalog == null || catalog.isEmpty()) {
      throw new VerdictDBDbmsException("Session catalog is not set.");
    }
  }
  
  @Override
  public List<String> getPartitionColumns(String schema, String table) 
      throws VerdictDBDbmsException {
    
    List<String> partition = new ArrayList<>();
    DbmsQueryResult queryResult = executeQuery(syntax.getPartitionCommand(schema, table));
    
    while (queryResult.next()) {
      String name = queryResult.getString(0);
      String extra = queryResult.getString(2);
      if (extra.contains("partition key")) {
        partition.add(name);
      }
    }
    
    return partition;
  }

  @Override
  public DbmsQueryResult executeSingle(String sql) throws VerdictDBDbmsException {
    log.debug("Issues the following query to DBMS: " + sql);

    PrestoQueryStatusPrinter progressMonitor = null;
    try {
      PrestoStatement stmt = (PrestoStatement) conn.createStatement();
      progressMonitor = new PrestoQueryStatusPrinter();
      stmt.setProgressMonitor(progressMonitor);
      setRunningStatement(stmt);
      JdbcQueryResult jrs = null;
      
      boolean doesResultExist = stmt.execute(sql);
      if (doesResultExist) {
        ResultSet rs = stmt.getResultSet();
        jrs = new JdbcQueryResult(rs);
        rs.close();
      } else {
        jrs = null;
      }
      progressMonitor.terminate();
      progressMonitor = null;
      setRunningStatement(null);
      
      stmt.close();
      return jrs;
      
    } catch (SQLException e) {
      if (isAborting) {
        return null;
      } else {
        String msg = "Issued the following query: " + sql + "\n" + e.getMessage();
        throw new VerdictDBDbmsException(msg);
      }
    } finally {
      // to handle the case that the query throws an error, but this progress monitor lingers.
      if (progressMonitor != null) {
        progressMonitor.terminate();
      }
    }
  }
  
}


/**
 * This is based on Presto-reported completed "splits".
 * 
 * Alternative, one can also retrieve the query progress from Presto's system table:
 * system.runtime.tasks
 * 
 * @author Yongjoo Park
 *
 */
class PrestoQueryStatusPrinter implements Consumer<QueryStats> {
  
  private ProgressBar pb = null;
  
  public void terminate() {
    if (pb != null) {
      pb.close();
      pb = null;
    }
  }

  @Override
  public void accept(QueryStats t) {
    String queryId = t.getQueryId();
    int total = t.getTotalSplits();
    int completed = t.getCompletedSplits();
    
    if (pb == null) {
      pb = new ProgressBar(queryId, total);
    }
    pb.maxHint(total);
    pb.stepTo(completed);
  }
}
