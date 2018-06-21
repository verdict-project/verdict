package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.connection.DbmsQueryResult;

public interface PostProcessor {
  
  DbmsQueryResult process(List<DbmsQueryResult> intermediates);

}
