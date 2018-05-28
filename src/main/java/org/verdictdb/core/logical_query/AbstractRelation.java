package org.verdictdb.core.logical_query;

import java.util.Optional;

/**
 * Represents a relation (or a table) that can appear in the from clause.
 * 
 * @author Yongjoo Park
 *
 */
public abstract class AbstractRelation {
  
  Optional<String> aliasName = Optional.empty();
  
  public void setAliasName(String aliasName) {
    this.aliasName = Optional.of(aliasName);
  }
  
  public Optional<String> getAliasName() {
    return aliasName;
  }

}
