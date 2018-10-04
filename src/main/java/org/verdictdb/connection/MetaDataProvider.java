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

package org.verdictdb.connection;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.exception.VerdictDBDbmsException;

import java.util.List;

public interface MetaDataProvider {

  public List<String> getSchemas() throws VerdictDBDbmsException;

  List<String> getTables(String schema) throws VerdictDBDbmsException;

  public List<Pair<String, String>> getColumns(String schema, String table)
      throws VerdictDBDbmsException;

  public List<String> getPartitionColumns(String schema, String table)
      throws VerdictDBDbmsException;

  public String getDefaultSchema();

  public void setDefaultSchema(String schema) throws VerdictDBDbmsException;
}
