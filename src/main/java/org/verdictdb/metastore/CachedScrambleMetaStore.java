package org.verdictdb.metastore;

import org.verdictdb.core.scrambling.ScrambleMetaSet;

public class CachedScrambleMetaStore extends VerdictMetaStore {
  
  VerdictMetaStore originalMetaStore;
  
  ScrambleMetaSet cachedMetaSet = null;
  
  public CachedScrambleMetaStore(VerdictMetaStore metaStore) {
    this.originalMetaStore = metaStore;
  }
  
  @Override
  public ScrambleMetaSet retrieve() {
    if (cachedMetaSet == null) {
      refreshCache();
    }
    
    return cachedMetaSet;
  }
  
  public void refreshCache() {
    cachedMetaSet = originalMetaStore.retrieve();
  }

}
