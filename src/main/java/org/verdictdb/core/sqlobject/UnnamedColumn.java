package org.verdictdb.core.sqlobject;

/**
 * Represents the column without alias.
 * 
 * @author Yongjoo Park
 *
 */
public interface UnnamedColumn extends GroupingAttribute, SelectItem {

  public UnnamedColumn deepcopy();

}
