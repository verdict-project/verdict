package org.verdictdb.core.scrambling;

import org.junit.Test;
import org.verdictdb.exception.VerdictDBValueException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ScrambleMetaSerializationTest {

  private ScrambleMeta createScrambleMeta() throws VerdictDBValueException {
    String scrambleSchemaName = "new_schema";
    String scrambleTableName = "New_Table";
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

    ScrambleMeta meta =
        new ScrambleMeta(
            scrambleSchemaName,
            scrambleTableName,
            originalSchemaName,
            originalTableName,
            blockColumn,
            blockCount,
            tierColumn,
            tierCount,
            cumulativeMassDistributionPerTier);

    return meta;
  }

  private String createSerializedScrambleMeta() {
    String jsonString =
        "{\"schemaName\":\"new_schema\",\"tableName\":\"New_Table\","
            + "\"originalSchemaName\":\"Original_Schema\",\"originalTableName\":\"origiNAL_TABLE\","
            + "\"aggregationBlockColumn\":\"verdictDBblock\",\"aggregationBlockCount\":3,"
            + "\"tierColumn\":\"VerdictTIER\",\"numberOfTiers\":2,\"method\":null,"
            + "\"scramblingMethod\":null,\"hashColumn\":null,"
            + "\"cumulativeDistributions\":{\"0\":[0.3,0.6,1.0],\"1\":[0.2,0.5,1.0]}}";
    return jsonString;
  }

  @Test
  public void testJsonSerialization() throws VerdictDBValueException {
    ScrambleMeta meta = createScrambleMeta();

    String jsonString = meta.toJsonString();
    //    System.out.println(jsonString);
    String expected = createSerializedScrambleMeta();
    assertEquals(expected, jsonString);
  }

  @Test
  public void testJsonDeserialization() throws VerdictDBValueException {
    String jsonString = createSerializedScrambleMeta();
    ScrambleMeta actual = ScrambleMeta.fromJsonString(jsonString);

    ScrambleMeta expected = createScrambleMeta();
    assertEquals(expected, actual);
  }
}
