package org.verdictdb.core.querying.ola;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.exception.VerdictDBValueException;

public class HyperTableCube {

  List<Dimension> dimensions = new ArrayList<>();   // serves as dimension constraints

  public HyperTableCube() {}

  public HyperTableCube(List<Dimension> dimensions) {
    this.dimensions = dimensions;
  }
  
  // TODO: use this method for creating a merged cube
  public static HyperTableCube createMergedCubes(HyperTableCube cube1, HyperTableCube cube2) {
    return null;
  }

  Dimension getDimension(int index) {
    return dimensions.get(index);
  }

  public List<Dimension> getDimensions() {
    return dimensions;
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
  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public boolean equals(Object obj) {
    return EqualsBuilder.reflectionEquals(this, obj);
  }

}
