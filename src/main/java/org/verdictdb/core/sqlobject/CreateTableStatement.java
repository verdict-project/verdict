package org.verdictdb.core.sqlobject;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Not implemented yet.
 * 
 * @author Yongjoo Park
 *
 */
public class CreateTableStatement implements SqlConvertible {

  private static final long serialVersionUID = -3733162210722527846L;

  String schemaName;

  String tableName;

  List<String> partitionColumns = new ArrayList<>();
  
  List<Pair<String, String>> columnNameAndTypes = new ArrayList<>();

}
