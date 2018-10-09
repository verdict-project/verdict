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

package org.verdictdb.core.querying.ola;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.exception.VerdictDBValueException;

/**
 * Represents a subset of table identified by the blockid declared on each table.
 * @author Yongjoo Park
 *
 */
public class HyperTableCube implements Serializable {

  private static final long serialVersionUID = -2326120491898400014L;

  List<Dimension> dimensions = new ArrayList<>(); // serves as dimension constraints

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
  
  public List<HyperTableCube> rippleJoinSlice() throws VerdictDBValueException {
    List<HyperTableCube> cubes = new ArrayList<>();
    List<Dimension> occupiedDims = new ArrayList<>();
    int dim = dimensions.size();
    for (int i = 0; i < dim; i++) {
      // set the last dimension 0 so that the sliceAt() will initially slice along that dimension
      Dimension thisDim = dimensions.get(i);
      if (i < dim - 1) {
        occupiedDims.add(
            new Dimension(
                thisDim.getSchemaName(), 
                thisDim.getTableName(), 
                thisDim.getBegin(), 
                thisDim.getBegin()));
      } else {
        occupiedDims.add(
            new Dimension(
                thisDim.getSchemaName(), 
                thisDim.getTableName(), 
                thisDim.getBegin(), 
                thisDim.getBegin()-1));
      }
    }
    HyperTableCube occupied = new HyperTableCube(occupiedDims);
    
    // In each iteration, a new slice (of type Cube) is created and added.
    while (true) { 
      Pair<HyperTableCube, HyperTableCube> sliceAndOccupied = sliceAt(occupied);
      if (sliceAndOccupied == null) {
        break;
      }
      HyperTableCube slice = sliceAndOccupied.getLeft();
      cubes.add(slice);
      occupied = sliceAndOccupied.getRight();
    }
    
    return cubes;
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
      for (int i = 0; i < dimensions.size(); i++) {
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
   * Chooses a block using the idea by ripple join (by Haas). A new cube is chosen using the
   * nexIndices, which is increased by one (at one dimension or equivalently, for one of its 
   * elements) after creating a new cube.
   * 
   * @param nexIndices
   * @return (NewCube, OccupiedCube)
   */
  Pair<HyperTableCube, HyperTableCube> sliceAt(HyperTableCube occupied) {
    // identify the dimension with the smallest value. if the values of all dimensions are equal
    // then, the last dimension will be chosen.
    int dimToExpand = -1;
    int smallestIndex = Integer.MAX_VALUE;   // the largest index value thus far observed
    int dim = dimensions.size();
    for (int i = 0; i < dim; i++) {
      int thisDimIndex = occupied.getDimension(i).getEnd();
      if (thisDimIndex >= dimensions.get(i).getEnd()) {
        // if the current end is at the last position, we should not increase this dimension.
        continue;
      }
      
      if (thisDimIndex <= smallestIndex) {
        smallestIndex = thisDimIndex;
        dimToExpand = i;
      }
    }
    
    // if there is no dimension to expand, return null to indicate that there is no more slice
    if (dimToExpand == -1) {
      return null;
    }
    
    // a new hypercube is a slice over the largestIndex while using the entire interval for each of
    // other dimensions.
    List<Dimension> newDimensions = new ArrayList<>();
    List<Dimension> expanded = new ArrayList<>();
    for (int i = 0; i < occupied.getDimensions().size(); i++) {
      Dimension d = occupied.getDimension(i);
      if (i == dimToExpand) {
        newDimensions.add(
            new Dimension(d.getSchemaName(), d.getTableName(), d.getEnd()+1, d.getEnd()+1));
        expanded.add(
            new Dimension(d.getSchemaName(), d.getTableName(), d.getBegin(), d.getEnd()+1));
      } else {
        newDimensions.add(d);
        expanded.add(d);
      }
    }
    
    return Pair.of(new HyperTableCube(newDimensions), new HyperTableCube(expanded));
  }

  /**
   * This class should be used only when there exists at least two dimensions whose lengths are
   * longer than one.
   *
   * @param dimIndex a slice dimension
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
        remaining.add(new Dimension(d.schemaName, d.tableName, d.begin + 1, d.end));
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
