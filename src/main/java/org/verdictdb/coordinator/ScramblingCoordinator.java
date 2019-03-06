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

package org.verdictdb.coordinator;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.ConcurrentJdbcConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
import org.verdictdb.core.scrambling.FastConvergeScramblingMethod;
import org.verdictdb.core.scrambling.HashScramblingMethod;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScramblingMethod;
import org.verdictdb.core.scrambling.ScramblingMethodBase;
import org.verdictdb.core.scrambling.ScramblingPlan;
import org.verdictdb.core.scrambling.UniformScramblingMethod;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.CreateSchemaQuery;
import org.verdictdb.core.sqlobject.CreateScrambleQuery;
import org.verdictdb.core.sqlobject.DropTableQuery;
import org.verdictdb.core.sqlobject.InsertIntoSelectQuery;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.sqlwriter.QueryToSql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

// When scrambling, UniformScramblingMethod determines blockSize, blockCount and actualBlockCount
// as follows:

// 1. scrambleTableSize = ceil(tableRowCount * relativeSize)
//
// 2. blockCount = ceil(tableRowCount / blockSize)
//
// 3. actualBlockCount = min(maxScrambleBlockCount, scrambleTableSize / blockSize)
//
// 4. blockSize = scrambleTableSize / actualBlockCount
//
// CREATE TABLE scrambledTable AS
// SELECT * FROM ( SELECT *, rand() * blockCount as verdictdbblock FROM originalTable) t
// WHERE verdictdbblock < actualBlockCount
//
public class ScramblingCoordinator {

  private final Set<String> scramblingMethods =
      new HashSet<>(Arrays.asList("uniform", "fastconverge"));

  // default options
  // Note that these options are actually all specified by the values in
  // ExecutionContext.generateScrambleQuery()
  private final Map<String, String> options =
      new HashMap<String, String>() {
        private static final long serialVersionUID = -4491518418086939738L;

        {
          put("tierColumnName", "verdictdbtier");
          put("blockColumnName", "verdictdbblock");
          put("scrambleTableSuffix", "_scrambled");
          put("minScrambleTableBlockSize", "1e6");
          put("createIfNotExists", "false");
          put("maxScrambleTableBlockCount", "100");
          put("existingPartitionColumns", "");
        }
      };

  Optional<String> scrambleSchema;

  DbmsConnection conn;

  Optional<String> scratchpadSchema;

  private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

  public ScramblingCoordinator(DbmsConnection conn) {
    this(conn, null);
  }

  public ScramblingCoordinator(DbmsConnection conn, String scrambleSchema) {
    this(conn, scrambleSchema, scrambleSchema); // uses the same schema
  }

  public ScramblingCoordinator(
      DbmsConnection conn, String scrambleSchema, String scratchpadSchema) {
    this(conn, scrambleSchema, scratchpadSchema, null, null);
  }

  public ScramblingCoordinator(
      DbmsConnection conn, String scrambleSchema, String scratchpadSchema, Long blockSize) {
    this.conn = conn;
    this.scratchpadSchema = Optional.fromNullable(scratchpadSchema);
    this.scrambleSchema = Optional.fromNullable(scrambleSchema);
    options.put(
        "minScrambleTableBlockSize", String.valueOf(conn.getSyntax().getRecommendedblockSize()));
    if (blockSize != null) {
      options.put("minScrambleTableBlockSize", String.valueOf(blockSize));
    }
  }

  public ScramblingCoordinator(
      DbmsConnection conn,
      String scrambleSchema,
      String scratchpadSchema,
      Long blockSize,
      List<String> existingPartitionColumns) {
    this.conn = conn;
    this.scratchpadSchema = Optional.fromNullable(scratchpadSchema);
    this.scrambleSchema = Optional.fromNullable(scrambleSchema);
    options.put(
        "minScrambleTableBlockSize", String.valueOf(conn.getSyntax().getRecommendedblockSize()));
    if (blockSize != null) {
      options.put("minScrambleTableBlockSize", String.valueOf(blockSize));
    }
    if (existingPartitionColumns != null) {
      options.put("existingPartitionColumns", Joiner.on(",").join(existingPartitionColumns));
    }
  }

  public ScrambleMeta scramble(String originalSchema, String originalTable)
      throws VerdictDBException {
    String newSchema;
    if (scrambleSchema.isPresent()) {
      newSchema = scrambleSchema.get();
    } else {
      newSchema = originalSchema;
    }
    String newTable = originalTable + options.get("scrambleTableSuffix");
    ScrambleMeta meta = scramble(originalSchema, originalTable, newSchema, newTable);
    return meta;
  }

  public ScrambleMeta scramble(
      String originalSchema, String originalTable, String newSchema, String newTable)
      throws VerdictDBException {

    String methodName = "uniform";
    String primaryColumn = null;
    ScrambleMeta meta =
        scramble(originalSchema, originalTable, newSchema, newTable, methodName, primaryColumn);
    return meta;
  }

  public ScrambleMeta scramble(
      String originalSchema,
      String originalTable,
      String newSchema,
      String newTable,
      String methodName)
      throws VerdictDBException {

    String primaryColumn = null;
    ScrambleMeta meta =
        scramble(originalSchema, originalTable, newSchema, newTable, methodName, primaryColumn);
    return meta;
  }

  public ScrambleMeta scramble(
      String originalSchema,
      String originalTable,
      String newSchema,
      String newTable,
      String methodName,
      String primaryColumn)
      throws VerdictDBException {

    // copied options
    Map<String, String> customOptions = new HashMap<>(options);

    ScrambleMeta meta =
        scramble(
            originalSchema,
            originalTable,
            newSchema,
            newTable,
            methodName,
            primaryColumn,
            1.0,
            null,
            customOptions);
    return meta;
  }

  // Note: this is the method currently used by the upstream interface.
  public void appendScramble(CreateScrambleQuery query) throws VerdictDBException {
    ScramblingMethod scramblingMethod = query.getScramblingMethod();
    String methodName = query.getMethod();
    String scrambleSchema = query.getNewSchema();
    String scrambleTable = query.getNewTable();
    String tempTable = "verdictdbtemp_" + RandomStringUtils.randomAlphanumeric(8);
    String originalSchema = query.getOriginalSchema();
    String originalTable = query.getOriginalTable();
    String primaryColumn = query.getHashColumnName();
    double relativeSize = scramblingMethod.getRelativeSize();

    // overwrite options with custom options.
    Map<String, String> effectiveOptions = new HashMap<>();
    for (Entry<String, String> o : options.entrySet()) {
      effectiveOptions.put(o.getKey(), o.getValue());
    }

    // perform scrambling
    log.info(
        String.format(
            "Starts to create a temporary %s scramble %s.%s from %s.%s",
            methodName.toUpperCase(), scrambleSchema, tempTable, originalSchema, originalTable));
    if (methodName.equalsIgnoreCase("hash")) {
      log.info(String.format("Method: %s on %s", methodName.toUpperCase(), primaryColumn));
    } else {
      log.info(String.format("Method: %s", methodName.toUpperCase()));
    }
    log.info(
        String.format(
            "Relative size: %.6f (or equivalently, %.4f %%)", relativeSize, relativeSize * 100));

    ScramblingPlan plan =
        ScramblingPlan.create(
            scrambleSchema,
            tempTable,
            originalSchema,
            originalTable,
            scramblingMethod,
            query.getWhere(),
            effectiveOptions);
    ExecutablePlanRunner.runTillEnd(conn, plan);
    log.info(
        String.format(
            "Finished creating temporary scramble to append: %s.%s", scrambleSchema, tempTable));

    List<Pair<String, String>> columns = conn.getColumns(scrambleSchema, scrambleTable);
    List<SelectItem> selectList = new ArrayList<>();
    for (Pair<String, String> column : columns) {
      String columnName = column.getLeft();
      selectList.add(new BaseColumn(columnName));
    }

    // insert temporary scramble to existing scramble
    InsertIntoSelectQuery insertQuery = new InsertIntoSelectQuery();
    SelectQuery selectQuery =
        SelectQuery.create(selectList, new BaseTable(scrambleSchema, tempTable));
    insertQuery.setSchemaName(scrambleSchema);
    insertQuery.setTableName(scrambleTable);
    insertQuery.setSelectQuery(selectQuery);

    String sql = QueryToSql.convert(conn.getSyntax(), insertQuery);
    conn.execute(sql);
    log.info(
        String.format(
            "Appended a temporary scramble {%s.%s} to the existing scramble (%s.%s)",
            scrambleSchema, tempTable, scrambleSchema, scrambleTable));

    // drop temporary scramble
    DropTableQuery dropQuery = new DropTableQuery(scrambleSchema, tempTable);
    sql = QueryToSql.convert(conn.getSyntax(), dropQuery);
    conn.execute(sql);
    log.info(
        String.format("Temporary scramble {%s.%s} has been dropped", scrambleSchema, tempTable));
  }

  // Note: this is the method currently used by the upstream interface.
  public ScrambleMeta scramble(CreateScrambleQuery query) throws VerdictDBException {

    String originalSchema = query.getOriginalSchema();
    String originalTable = query.getOriginalTable();
    String newSchema = query.getNewSchema();
    String newTable = query.getNewTable();
    String methodName = query.getMethod();
    String primaryColumn = query.getHashColumnName();
    double relativeSize = query.getSize();
    Map<String, String> customOptions = new HashMap<>(options);
    customOptions.put("minScrambleTableBlockSize", Long.toString(query.getBlockSize()));

    ScrambleMeta meta =
        scramble(
            originalSchema,
            originalTable,
            newSchema,
            newTable,
            methodName,
            primaryColumn,
            relativeSize,
            query.getWhere(),
            customOptions);

    return meta;
  }

  public ScrambleMeta scramble(
      String originalSchema,
      String originalTable,
      String newSchema,
      String newTable,
      String methodName,
      String primaryColumn,
      Map<String, String> customOptions)
      throws VerdictDBException {
    ScrambleMeta meta =
        scramble(
            originalSchema,
            originalTable,
            newSchema,
            newTable,
            methodName,
            primaryColumn,
            1.0,
            null,
            customOptions);
    return meta;
  }

  /**
   * @param originalSchema Original schema name
   * @param originalTable Original table name
   * @param newSchema Scramble schema name
   * @param newTable Scramble table name
   * @param methodName Either 'uniform' or 'hash'
   * @param primaryColumn Passes hashcolumn for hash sampling.
   * @param relativeSize The ratio of a scramble in comparison to the original table.
   * @param where condition to be used for creating a scramble
   * @param customOptions
   * @return
   * @throws VerdictDBException
   */
  // scramble(CreateScrambleQuery query) method relies on this.
  public ScrambleMeta scramble(
      String originalSchema,
      String originalTable,
      String newSchema,
      String newTable,
      String methodName,
      String primaryColumn,
      double relativeSize,
      UnnamedColumn where,
      Map<String, String> customOptions)
      throws VerdictDBException {

    // this check is now performed by ScramblingQuery
    //    // sanity check
    //    if (!scramblingMethods.contains(methodName.toLowerCase())) {
    //      throw new VerdictDBValueException("Not supported scrambling method: " + methodName);
    //    }

    // create a schema if not exists
    if (!conn.getSchemas().contains(newSchema)) {
      CreateSchemaQuery createSchemaQuery = new CreateSchemaQuery(newSchema);
      createSchemaQuery.setIfNotExists(true);
      String sql = QueryToSql.convert(conn.getSyntax(), createSchemaQuery);
      conn.execute(sql);
    }

    // overwrite options with custom options.
    Map<String, String> effectiveOptions = new HashMap<String, String>();
    for (Entry<String, String> o : options.entrySet()) {
      effectiveOptions.put(o.getKey(), o.getValue());
    }
    for (Entry<String, String> o : customOptions.entrySet()) {
      effectiveOptions.put(o.getKey(), o.getValue());
    }

    // determine scrambling method
    long blockSize = Double.valueOf(effectiveOptions.get("minScrambleTableBlockSize")).longValue();
    int maxBlockCount =
        Double.valueOf(effectiveOptions.get("maxScrambleTableBlockCount")).intValue();
    ScramblingMethod scramblingMethod;
    ScramblingMethodBase scramblingMethodBase;
    if (methodName.equalsIgnoreCase("uniform")) {
      scramblingMethodBase = new UniformScramblingMethod(blockSize, maxBlockCount, relativeSize);
    } else if (methodName.equalsIgnoreCase("hash")) {
      scramblingMethodBase =
          new HashScramblingMethod(blockSize, maxBlockCount, relativeSize, primaryColumn);
    } else if (methodName.equalsIgnoreCase("FastConverge") && primaryColumn == null) {
      scramblingMethodBase = new FastConvergeScramblingMethod(blockSize, scratchpadSchema.get());
    } else if (methodName.equalsIgnoreCase("FastConverge") && primaryColumn != null) {
      scramblingMethodBase =
          new FastConvergeScramblingMethod(blockSize, scratchpadSchema.get(), primaryColumn);
    } else {
      throw new VerdictDBValueException("Invalid scrambling method: " + methodName);
    }
    scramblingMethod = scramblingMethodBase;

    // perform scrambling
    log.info(
        String.format(
            "Starts to create a new %s scramble %s.%s from %s.%s",
            methodName.toUpperCase(), newSchema, newTable, originalSchema, originalTable));
    if (methodName.equalsIgnoreCase("hash")) {
      log.info(String.format("Method: %s on %s", methodName.toUpperCase(), primaryColumn));
    } else {
      log.info(String.format("Method: %s", methodName.toUpperCase()));
    }
    log.info(
        String.format(
            "Relative size: %.6f (or equivalently, %.4f %%)", relativeSize, relativeSize * 100));

    ScramblingPlan plan =
        ScramblingPlan.create(
            newSchema,
            newTable,
            originalSchema,
            originalTable,
            scramblingMethod,
            where,
            effectiveOptions);
    ExecutablePlanRunner.runTillEnd(conn, plan);
    log.info(String.format("Finished creating %s.%s", newSchema, newTable));

    // Reinitiate Connections after table creation is done
    // This is to handle the case that the JDBC connections are disconnected due to
    // the long idle time.
    if (conn instanceof ConcurrentJdbcConnection) {
      ((ConcurrentJdbcConnection) conn).reinitiateConnection();
    } else if (conn instanceof CachedDbmsConnection
        && ((CachedDbmsConnection) conn).getOriginalConnection()
            instanceof ConcurrentJdbcConnection) {
      ((ConcurrentJdbcConnection) ((CachedDbmsConnection) conn).getOriginalConnection())
          .reinitiateConnection();
    }

    // compose scramble meta
    String tierColumn = effectiveOptions.get("tierColumnName");
    int tierCount = scramblingMethod.getTierCount();
    Map<Integer, List<Double>> cumulativeDistribution = new HashMap<>();
    for (int i = 0; i < tierCount; i++) {
      List<Double> dist = scramblingMethod.getStoredCumulativeProbabilityDistributionForTier(i);
      cumulativeDistribution.put(i, dist);
    }
    String blockColumn = effectiveOptions.get("blockColumnName");
    int blockCount = scramblingMethod.getActualBlockCount();

    ScrambleMeta meta =
        new ScrambleMeta(
            newSchema,
            newTable,
            originalSchema,
            originalTable,
            blockColumn,
            blockCount,
            tierColumn,
            tierCount,
            cumulativeDistribution,
            methodName,
            primaryColumn,
            scramblingMethodBase);

    return meta;
  }
}
