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

  String schemaName;

  String tableName;

  List<String> partitionColumns = new ArrayList<>();
  
  List<Pair<String, String>> columnNameAndTypes = new ArrayList<>();

}
