package org.verdictdb.core.connection;

import java.util.ArrayList;
import java.util.List;

public class DbmsQueryResultMetaData {

  public List<Boolean> isCurrency = new ArrayList<>();

  public List<Integer> isNullable = new ArrayList<>();

  public List<Integer> precision = new ArrayList<>();

  public List<Integer> scale = new ArrayList<>();

  public List<Integer> columnDisplaySize = new ArrayList<>();

  public List<Boolean> isAutoIncrement = new ArrayList<>();

  public List<String> columnClassName = new ArrayList<>();

  public DbmsQueryResultMetaData(){}
}
