package org.verdictdb.core.sqlobject;

public class CreateScrambleQuery extends CreateTableQuery {

  private static final long serialVersionUID = -6363349381526760468L;
  
  private String newSchema;
  
  private String newTable;
  
  private String originalSchema;
  
  private String originalTable;
  
  private String method;
  
  private double size = 1.0;    // in fraction
  
  public CreateScrambleQuery() {}

  public CreateScrambleQuery(
      String newSchema, String newTable, 
      String originalSchema, String originalTable,
      String method, double size) {
    super();
    this.newSchema = newSchema;
    this.newTable = newTable;
    this.originalSchema = originalSchema;
    this.originalTable = originalTable;
    this.method = method;
    this.size = size;
  }

  public String getNewSchema() {
    return newSchema;
  }

  public String getNewTable() {
    return newTable;
  }

  public String getOriginalSchema() {
    return originalSchema;
  }

  public String getOriginalTable() {
    return originalTable;
  }

  public String getMethod() {
    return method;
  }

  public double getSize() {
    return size;
  }

  public void setNewSchema(String newSchema) {
    this.newSchema = newSchema;
  }

  public void setNewTable(String newTable) {
    this.newTable = newTable;
  }

  public void setOriginalSchema(String originalSchema) {
    this.originalSchema = originalSchema;
  }

  public void setOriginalTable(String originalTable) {
    this.originalTable = originalTable;
  }

  public void setMethod(String method) {
    this.method = method;
  }

  public void setSize(double size) {
    this.size = size;
  }
  
  

}
