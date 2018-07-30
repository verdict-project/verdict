package org.verdictdb.core.querying.ola;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBValueException;

public class AggMetaTest {
  
  //@Test
  public void testTierCombinationTwoTableMultiTier() throws VerdictDBValueException {
    String schemaName1 = "new_schema1";
    String tableName1 = "new_table1";
    String schemaName2 = "new_schema2";
    String tableName2 = "new_table2";
    ScrambleMeta meta1 = createTwoTierScrambleMeta(schemaName1, tableName1);
    ScrambleMeta meta2 = createTwoTierScrambleMeta(schemaName2, tableName2);
    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(0, 0), Arrays.asList(1, 0), Arrays.asList(0, 1), Arrays.asList(1, 1));
    compareTierCombinationSets(expected, Arrays.asList(meta1, meta2));
  }
  
  @Test
  public void testTierCombinationTwoTableSingleTier() throws VerdictDBValueException {
    String schemaName1 = "new_schema1";
    String tableName1 = "new_table1";
    String schemaName2 = "new_schema2";
    String tableName2 = "new_table2";
    ScrambleMeta meta1 = createSingleTierScrambleMeta(schemaName1, tableName1);
    ScrambleMeta meta2 = createSingleTierScrambleMeta(schemaName2, tableName2);
    List<List<Integer>> expected = Arrays.asList(Arrays.asList(0, 0));
    compareTierCombinationSets(expected, Arrays.asList(meta1, meta2));
  }
  
  @Test
  public void testTierCombinationSingleTableMultiTier() throws VerdictDBValueException {
    String schemaName = "new_schema";
    String tableName = "new_table";
    ScrambleMeta meta = createTwoTierScrambleMeta(schemaName, tableName);
    List<List<Integer>> expected = Arrays.asList(Arrays.asList(0), Arrays.asList(1));
    compareTierCombinationSets(expected, Collections.singleton(meta));
  }
  
  @Test
  public void testTierCombinationSingleTableSingleTier() throws VerdictDBValueException {
    String schemaName = "new_schema";
    String tableName = "new_table";
    ScrambleMeta meta = createSingleTierScrambleMeta(schemaName, tableName);
    List<List<Integer>> expected = Arrays.asList(Arrays.asList(0));
    compareTierCombinationSets(expected, Collections.singleton(meta));
  }

  private void compareTierCombinationSets(List<List<Integer>> expected, Collection<ScrambleMeta> metas) {
    ScrambleMetaSet metaset = ScrambleMetaSet.createFromCollection(metas);
    AggMeta aggmeta = new AggMeta();
    List<TierCombination> combinations = aggmeta.generateAllTierCombinations(metaset);
    List<List<Integer>> tierCombSet = new ArrayList<>();
    for (TierCombination c : combinations) {
      List<Integer> tierNoSeries = new ArrayList<>();
      for (Entry<Pair<String, String>, Integer> cToNo : c) {
        int tierNumber = cToNo.getValue();
        tierNoSeries.add(tierNumber);
      }
      tierCombSet.add(tierNoSeries);
    }
    assertEquals(expected, tierCombSet);

  }

  @Test
  public void testSingleTierNoJoinCombination() throws VerdictDBValueException {
  
    String schemaName = "new_schema";
    String tableName = "new_table";
    
    AggMeta aggmeta = new AggMeta();
    Dimension dim = new Dimension(schemaName, tableName, 0, 0);
    HyperTableCube cube = new HyperTableCube(Arrays.asList(dim));
    ScrambleMeta meta = createSingleTierScrambleMeta(schemaName, tableName);
    aggmeta.addCube(cube);
    aggmeta.addScrambleTableTierColumnAlias(meta, "tier_alias");
  
    Map<TierCombination, Double> tierToScaleFactor = aggmeta.computeScaleFactors();
    TierCombination testtier = new TierCombination(
        Arrays.asList(Pair.of(schemaName, tableName)),
        Arrays.asList(0));
    assertEquals(1.0/0.3, tierToScaleFactor.get(testtier), 1e-6);
  }
  
  @Test
  public void testSingleTierJoinCombination() throws VerdictDBValueException {
    
    String schemaName1 = "new_schema1";
    String tableName1 = "new_table1";
    String schemaName2 = "new_schema2";
    String tableName2 = "new_table2";
    
    // setup
    AggMeta aggmeta = new AggMeta();
    Dimension dim1 = new Dimension(schemaName1, tableName1, 0, 0);
    Dimension dim2 = new Dimension(schemaName2, tableName2, 1, 1);
    HyperTableCube cube = new HyperTableCube(Arrays.asList(dim1, dim2));
    ScrambleMeta meta1 = createSingleTierScrambleMeta(schemaName1, tableName1);
    ScrambleMeta meta2 = createSingleTierScrambleMeta(schemaName2, tableName2);
    aggmeta.addCube(cube);
    aggmeta.addScrambleTableTierColumnAlias(meta1, "tier_alias1");
    aggmeta.addScrambleTableTierColumnAlias(meta2, "tier_alias2");
  
    // test
    Map<TierCombination, Double> tierToScaleFactor = aggmeta.computeScaleFactors();
    TierCombination testtier = new TierCombination(
        Arrays.asList(Pair.of(schemaName1, tableName1), Pair.of(schemaName2, tableName2)),
        Arrays.asList(0, 0));
    assertEquals(1.0 / (0.3*0.3), tierToScaleFactor.get(testtier), 1e-6);
  }
  
  private ScrambleMeta createSingleTierScrambleMeta(
      String scrambleSchemaName, String scrambleTableName
  ) throws VerdictDBValueException {
  
//    String scrambleSchemaName = "new_schema";
//    String scrambleTableName = "new_table";
    String originalSchemaName = "original_schema";
    String originalTableName = "original_table";
    String blockColumn = "vblock";
    int blockCount = 3;
    String tierColumn = "vtier";
    int tierCount = 1;
  
    Map<Integer, List<Double>> cumulativeMassDistributionPerTier = new HashMap<>();
    List<Double> dist0 = Arrays.asList(0.3, 0.6, 1.0);
    cumulativeMassDistributionPerTier.put(0, dist0);
  
    ScrambleMeta meta = new ScrambleMeta(
        scrambleSchemaName,  scrambleTableName,
        originalSchemaName,  originalTableName,
        blockColumn,  blockCount,
        tierColumn,  tierCount,
        cumulativeMassDistributionPerTier);
  
    return meta;
  }
  
  private ScrambleMeta createTwoTierScrambleMeta(String scrambleSchemaName, String scrambleTableName) 
      throws VerdictDBValueException {
    String originalSchemaName = "Original_Schema";
    String originalTableName = "origiNAL_TABLE";
    String blockColumn = "verdictDBblock";
    int blockCount = 3;
    String tierColumn = "VerdictTIER";
    int tierCount = 2;
    
    Map<Integer, List<Double>> cumulativeMassDistributionPerTier = new HashMap<>();
    List<Double> dist0 = Arrays.asList(0.3, 0.6, 1.0);
    List<Double> dist1 = Arrays.asList(0.2, 0.5, 1.0);
    cumulativeMassDistributionPerTier.put(0, dist0);
    cumulativeMassDistributionPerTier.put(1, dist1);
    
    ScrambleMeta meta = new ScrambleMeta(
        scrambleSchemaName,  scrambleTableName,
        originalSchemaName,  originalTableName,
        blockColumn,  blockCount,
        tierColumn,  tierCount,
        cumulativeMassDistributionPerTier);
    
    return meta;
  }

}
