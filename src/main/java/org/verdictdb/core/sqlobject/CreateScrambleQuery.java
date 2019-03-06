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

package org.verdictdb.core.sqlobject;

import org.verdictdb.core.scrambling.ScramblingMethod;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;
import org.verdictdb.sqlsyntax.SqlSyntax;

import java.util.List;

public class CreateScrambleQuery extends CreateTableQuery {

  private static final long serialVersionUID = -6363349381526760468L;

  private String newSchema;

  private String newTable;

  private String originalSchema;

  private String originalTable;

  /**
   * One of the following:
   *
   * <ol>
   *   <li>1. uniform
   *   <li>2. hash
   * </ol>
   */
  private String method;

  /** the total number of tuples in relation to that of the original table (in fraction) */
  private double size = 1.0;

  /** the number of tuples for each block */
  private long blocksize;

  /** The column (if present) used for hashed sampling */
  private String hashColumnName = null;

  /** the condition that will be used to create a scramble */
  private UnnamedColumn where = null;

  /** the scrambling method */
  private ScramblingMethod scramblingMethod = null;

  /** Existing partition columns in the original table */
  private List<String> existingPartitionColumns;

  public CreateScrambleQuery() {}

  public CreateScrambleQuery(
      String newSchema,
      String newTable,
      String originalSchema,
      String originalTable,
      String method,
      double size,
      long blocksize,
      String hashColumnName,
      UnnamedColumn where) {
    super();
    this.newSchema = newSchema;
    this.newTable = newTable;
    this.originalSchema = originalSchema;
    this.originalTable = originalTable;
    this.method = method;
    this.size = size;
    this.blocksize = blocksize;
    this.hashColumnName = hashColumnName;
    this.where = where;
  }

  public void setExistingPartitionColumns(List<String> existingPartitionColumns) {
    this.existingPartitionColumns = existingPartitionColumns;
  }

  /**
   * Checks if the field values are proper.
   *
   * @return True if this query is logically valid.
   * @param syntax
   */
  public void checkIfSupported(SqlSyntax syntax) throws VerdictDBValueException {
    if (method.equalsIgnoreCase("uniform")
        || method.equalsIgnoreCase("hash")
        || method.equalsIgnoreCase("FastConverge")) {
    } else {
      throw new VerdictDBValueException(
          String.format(
              "The scrambling method is set to %s."
                  + "The scrambling method must be either uniform or hash.",
              method));
    }

    if (method.equals("hash") && hashColumnName == null) {
      throw new VerdictDBValueException(
          "The hash column is null."
              + "If the scrambling method is hash, "
              + "hash column name must be present.");
    }

    if (size <= 0 || size > 1) {
      throw new VerdictDBValueException(
          String.format("Scramble size is %f. It must be between 0.0 and 1.0.", size));
    }

    if (blocksize == 0) {
      throw new VerdictDBValueException(
          String.format(
              "The scramble block size is set to 0."
                  + "A scramble block size should be greater than zero."));
    }

    if ((syntax instanceof PostgresqlSyntax || syntax instanceof MysqlSyntax)
        && existingPartitionColumns != null
        && !existingPartitionColumns.isEmpty()) {
      throw new VerdictDBValueException(
          "Creating a scramble for already partitioned tables in "
              + "PostgreSQL or MySQL is not supported.");
    }
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

  public long getBlockSize() {
    return blocksize;
  }

  public String getHashColumnName() {
    return hashColumnName;
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

  public long getBlocksize() {
    return blocksize;
  }

  public void setHashColumnName(String hashColumnName) {
    this.hashColumnName = hashColumnName;
  }

  public UnnamedColumn getWhere() {
    return where;
  }

  public void setWhere(UnnamedColumn where) {
    this.where = where;
  }

  public ScramblingMethod getScramblingMethod() {
    return scramblingMethod;
  }

  public void setScramblingMethod(ScramblingMethod scramblingMethod) {
    this.scramblingMethod = scramblingMethod;
  }

  public List<String> getExistingPartitionColumns() {
    return existingPartitionColumns;
  }
}
