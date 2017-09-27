/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Modified BSD License
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at:
//
// http://opensource.org/licenses/BSD-3-Clause
*/
package sqlline;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * List of all database connections in the current sqlline session.
 */
class DatabaseConnections implements Iterable<DatabaseConnection> {
  private final List<DatabaseConnection> connections =
      new ArrayList<DatabaseConnection>();
  private int index = -1;

  public DatabaseConnection current() {
    if (index != -1) {
      return connections.get(index);
    }

    return null;
  }

  public int size() {
    return connections.size();
  }

  public Iterator<DatabaseConnection> iterator() {
    return connections.iterator();
  }

  public void remove() {
    if (index != -1) {
      connections.remove(index);
    }

    while (index >= connections.size()) {
      index--;
    }
  }

  public void setConnection(DatabaseConnection connection) {
    if (connections.indexOf(connection) == -1) {
      connections.add(connection);
    }

    index = connections.indexOf(connection);
  }

  public int getIndex() {
    return index;
  }

  public boolean setIndex(int index) {
    if (index < 0 || index >= connections.size()) {
      return false;
    }

    this.index = index;
    return true;
  }
}

// End DatabaseConnections.java
