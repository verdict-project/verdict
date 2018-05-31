package org.verdictdb.core.query;

public class OrderbyAttribute {
  
  AliasReference aliasName;
  
  String order = "asc";
  
  public OrderbyAttribute(String attributeName) {
    this.aliasName = new AliasReference(attributeName);
  }
  
  public OrderbyAttribute(String attributeName, String order) {
    this.aliasName = new AliasReference(attributeName);
    this.order = order;
  }
  
  public String getAttributeName() {
    return aliasName.getAliasName();
  }
  
  public String getOrder() {
    return order;
  }
  
}
