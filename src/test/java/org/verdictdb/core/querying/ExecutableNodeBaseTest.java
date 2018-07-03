package org.verdictdb.core.querying;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class ExecutableNodeBaseTest {

  @Test
  public void testEquals() {
    ExecutableNodeBase a = new ExecutableNodeBase();
    ExecutableNodeBase b = a;
    assertEquals(b, a);
  }
  
  @Test
  public void testContains() {
    Map<ExecutableNodeBase, Integer> mymap = new HashMap<>();
    ExecutableNodeBase a = new ExecutableNodeBase();
    mymap.put(a, 3);
    assertTrue(mymap.containsKey(a));
  }

}
