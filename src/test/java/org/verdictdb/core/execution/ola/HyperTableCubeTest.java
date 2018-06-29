package org.verdictdb.core.execution.ola;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.verdictdb.exception.VerdictDBValueException;

public class HyperTableCubeTest {
  
  @Test
  public void testSliceAlong() throws VerdictDBValueException {
    Dimension d1 = new Dimension("myschema", "mytable", 1, 4);
    Dimension d2 = new Dimension("myschema", "mytable", 1, 3);
    HyperTableCube cube = new HyperTableCube(Arrays.asList(d1, d2));
    Pair<HyperTableCube, HyperTableCube> sliceAndLeft = cube.sliceAlong(0);
    System.out.println("slice: " + sliceAndLeft.getLeft());
    System.out.println("left: " + sliceAndLeft.getRight());
    assertEquals(2, sliceAndLeft.getRight().getDimension(0).begin);
    
    sliceAndLeft = cube.sliceAlong(1);
    System.out.println("slice: " + sliceAndLeft.getLeft());
    System.out.println("left: " + sliceAndLeft.getRight());
    assertEquals(2, sliceAndLeft.getRight().getDimension(1).begin);
  }

  @Test
  public void testSlicingTwoDim() throws VerdictDBValueException {
    Dimension d1 = new Dimension("myschema", "mytable", 1, 4);
    Dimension d2 = new Dimension("myschema", "mytable", 1, 3);
    HyperTableCube cube = new HyperTableCube(Arrays.asList(d1, d2));
    
    List<HyperTableCube> slices = cube.roundRobinSlice();
    System.out.println(slices);
    assertEquals(4, slices.size());
    assertEquals(1, slices.get(0).getDimension(0).begin);
    assertEquals(4, slices.get(0).getDimension(0).end);
    assertEquals(1, slices.get(0).getDimension(1).begin);
    assertEquals(1, slices.get(0).getDimension(1).end);
    assertEquals(2, slices.get(3).getDimension(0).begin);
    assertEquals(4, slices.get(3).getDimension(0).end);
    assertEquals(3, slices.get(3).getDimension(1).begin);
    assertEquals(3, slices.get(3).getDimension(1).end);
  }
  
  @Test
  public void testSlicingThreeDim() throws VerdictDBValueException {
    Dimension d1 = new Dimension("myschema", "mytable", 1, 4);
    Dimension d2 = new Dimension("myschema", "mytable", 1, 3);
    Dimension d3 = new Dimension("myschema", "mytable", 1, 2);
    HyperTableCube cube = new HyperTableCube(Arrays.asList(d1, d2, d3));
    
    List<HyperTableCube> slices = cube.roundRobinSlice();
    System.out.println(slices);
    assertEquals(5, slices.size());
    assertEquals(1, slices.get(0).getDimension(0).begin);
    assertEquals(4, slices.get(0).getDimension(0).end);
    assertEquals(1, slices.get(0).getDimension(1).begin);
    assertEquals(3, slices.get(0).getDimension(1).end);
    assertEquals(1, slices.get(0).getDimension(2).begin);
    assertEquals(1, slices.get(0).getDimension(2).end);
    
    assertEquals(2, slices.get(4).getDimension(0).begin);
    assertEquals(4, slices.get(4).getDimension(0).end);
    assertEquals(3, slices.get(4).getDimension(1).begin);
    assertEquals(3, slices.get(4).getDimension(1).end);
    assertEquals(2, slices.get(4).getDimension(2).begin);
    assertEquals(2, slices.get(4).getDimension(2).end);
  }

}
