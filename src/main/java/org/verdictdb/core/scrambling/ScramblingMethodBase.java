package org.verdictdb.core.scrambling;

public abstract class ScramblingMethodBase implements ScramblingMethod {

  protected final long blockSize;
  
  public ScramblingMethodBase(long blockSize) {
    this.blockSize = blockSize;
  }
  
  long getBlockSize() {
    return blockSize;
  }

}
