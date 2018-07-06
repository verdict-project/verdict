package org.verdictdb.core.scrambling;

import java.util.List;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.sqlobject.UnnamedColumn;

public abstract class ScramblingMethodBase implements ScramblingMethod {

  protected final long blockSize;
  
  public ScramblingMethodBase(long blockSize) {
    this.blockSize = blockSize;
  }
  
  long getBlockSize() {
    return blockSize;
  }

}
