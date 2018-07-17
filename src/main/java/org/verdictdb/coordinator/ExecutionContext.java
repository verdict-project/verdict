package org.verdictdb.coordinator;

import org.verdictdb.VerdictContext;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.parser.VerdictSQLParser;
import org.verdictdb.sqlreader.NonValidatingSQLParser;

/**
 * Stores the context for a single query execution. Includes both scrambling query and select query.
 * 
 * @author Yongjoo Park
 *
 */
public class ExecutionContext {
  
  private VerdictContext context;
  
  private final long serialNumber;
  
  private enum QueryType {
    select, scrambling, unknown
  }
  
  /**
   * 
   * @param context Parent context
   * @param contextId
   */
  public ExecutionContext(VerdictContext context, long serialNumber) {
    this.context = context;
    this.serialNumber = serialNumber;
  }
  
  public long getExecutionContextSerialNumber() {
    return serialNumber;
  }
  
  public VerdictSingleResult sql(String query) throws VerdictDBException {
    VerdictResultStream stream = streamsql(query);
    VerdictSingleResult result = stream.next();
    stream.close();
    return result;
  }
  
  public VerdictResultStream streamsql(String query) throws VerdictDBException {
    // determines the type of the given query and forward it to an appropriate coordinator.
    
    QueryType queryType = identifyQueryType(query);
    
    if (queryType.equals(QueryType.select)) {
      SelectQueryCoordinator coordinator = new SelectQueryCoordinator(context.getConnection());
      ExecutionResultReader reader = coordinator.process(query);
      VerdictResultStream stream = new VerdictResultStream(reader, this);
      return stream;
    }
    else if (queryType.equals(QueryType.scrambling)) {
      ScramblingCoordinator coordinator = new ScramblingCoordinator(context.getConnection());
      
      return null;
    }
    else {
      throw new VerdictDBTypeException("Unexpected type of query: " + query);
    }
  }

  /**
   * Terminates existing threads. The created database tables may still exist for successive uses.
   */
  public void terminate() {
    // TODO Auto-generated method stub
    
  }
  
  private QueryType identifyQueryType(String query) {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    
    if (parser.select_statement() != null) {
      return QueryType.select;
    } else if (parser.create_scramble_statement() != null) {
      return QueryType.scrambling;
    } else {
      return QueryType.unknown;
    }
  }

}
