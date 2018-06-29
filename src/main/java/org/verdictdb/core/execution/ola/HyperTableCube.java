package org.verdictdb.core.execution.ola;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.exception.VerdictDBValueException;

class HyperTableCube {

  List<Dimension> dimensions = new ArrayList<>();   // serves as dimension constraints

  public HyperTableCube() {}

  public HyperTableCube(List<Dimension> dimensions) {
    this.dimensions = dimensions;
  }

  Dimension getDimension(int index) {
    return dimensions.get(index);
  }

  public Pair<Integer, Integer> getSpanOf(String schemaName, String tableName) {
    for (Dimension d : dimensions) {
      if (d.schemaName.equals(schemaName) && d.tableName.equals(tableName)) {
        return Pair.of(d.begin, d.end);
      }
    }
    return null;
  }

  public List<HyperTableCube> roundRobinSlice() throws VerdictDBValueException {
    List<HyperTableCube> cubes = new ArrayList<>();
    HyperTableCube remaining = this;

    int tableIndex = dimensions.size() - 1;
    while (true) {
      int numberOfDimensionsLongerThanOne = 0;
      for (int i = 0; i < remaining.dimensions.size(); i++) {
        if (remaining.dimensions.get(i).length() > 1) {
          numberOfDimensionsLongerThanOne++;
        }
      }
      if (numberOfDimensionsLongerThanOne < 1) {
        cubes.add(remaining);
        break;
      }

      Pair<HyperTableCube, HyperTableCube> sliceAndLeft = remaining.sliceAlong(tableIndex);
      if (sliceAndLeft == null) {
        throw new VerdictDBValueException("Incorrect indexing.");
      }
      HyperTableCube slice = sliceAndLeft.getLeft();
      remaining = sliceAndLeft.getRight();
      cubes.add(slice);

      // search for the index at which the length of dimension is longer than 1.
      for(int i = 0; i < dimensions.size(); i++) {
        tableIndex--;
        if (tableIndex < 0) {
          tableIndex = dimensions.size() - 1;
        }
        if (remaining.dimensions.get(tableIndex).length() > 1) {
          break;
        }
      }

//      try {
//        System.out.println("cube " + cubes);
//        TimeUnit.SECONDS.sleep(1);
//      } catch (InterruptedException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//      }
    }

    return cubes;
  }

  /**
   *
   * This class should be used only when there exists at least two dimensions whose lengths are longer than
   * one.
   *
   * @param dimIndex  a slice dimension
   * @return (SlidedCube, RemainingCube)
   */
  Pair<HyperTableCube, HyperTableCube> sliceAlong(int dimIndex) {
    // base conditions
    if (dimIndex >= dimensions.size()) {
      return null;
    }
    if (dimensions.get(dimIndex).length() <= 1) {
      return null;
    }

    // regular slicing
    List<Dimension> slice = new ArrayList<>();
    List<Dimension> remaining = new ArrayList<>();
    for (int i = 0; i < dimensions.size(); i++) {
      Dimension d = dimensions.get(i);
      if (i != dimIndex) {
        slice.add(d);
        remaining.add(d);
      } else {
        slice.add(new Dimension(d.schemaName, d.tableName, d.begin, d.begin));
        remaining.add(new Dimension(d.schemaName, d.tableName, d.begin+1, d.end));
      }
    }

    return Pair.of(new HyperTableCube(slice), new HyperTableCube(remaining));
  }

  public boolean isEmpty() {
    return dimensions.isEmpty();
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}

class Dimension {

  String schemaName;

  String tableName;

  int begin;

  int end;

  public Dimension(String schemaName, String tableName, int begin, int end) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.begin = begin;
    this.end = end;
  }

  public int length() {
    return end - begin + 1;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
