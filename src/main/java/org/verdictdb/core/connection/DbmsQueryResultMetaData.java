package org.verdictdb.core.connection;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DbmsQueryResultMetaData implements Serializable {

  private static final long serialVersionUID = 472824664812257672L;

  public List<Boolean> isCurrency = new ArrayList<>();

  public List<Integer> isNullable = new ArrayList<>();

  public List<Integer> precision = new ArrayList<>();

  public List<Integer> scale = new ArrayList<>();

  public List<Integer> columnDisplaySize = new ArrayList<>();

  public List<Boolean> isAutoIncrement = new ArrayList<>();

  public List<String> columnClassName = new ArrayList<>();

  public DbmsQueryResultMetaData(){}
}
