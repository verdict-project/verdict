/*
 *    Copyright 2017 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.core.sqlobject;

public class CreateScrambleQuery extends CreateTableQuery {

  private static final long serialVersionUID = -6363349381526760468L;

  private String newSchema;

  private String newTable;

  private String originalSchema;

  private String originalTable;

  private String method;

  private double size = 1.0; // in fraction

  public CreateScrambleQuery() {}

  public CreateScrambleQuery(
      String newSchema,
      String newTable,
      String originalSchema,
      String originalTable,
      String method,
      double size) {
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
