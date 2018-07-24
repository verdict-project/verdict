/*
 *    Copyright 2018 University of Michigan
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

package org.verdictdb.sqlsyntax;

public class SqliteSyntax extends SqlSyntax {

  @Override
  public boolean doesSupportTablePartitioning() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void dropTable(String schema, String tablename) {
    // TODO Auto-generated method stub
  }

  @Override
  public String getFallbackDefaultSchema() {
    return "main";
  }

  @Override
  public int getColumnNameColumnIndex() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getColumnTypeColumnIndex() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getPartitionByInCreateTable() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getPartitionCommand(String schema, String table) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getQuoteString() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getSchemaCommand() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getSchemaNameColumnIndex() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getTableCommand(String schema) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getTableNameColumnIndex() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String randFunction() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isAsRequiredBeforeSelectInCreateTable() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }
    return true;
  }
}
