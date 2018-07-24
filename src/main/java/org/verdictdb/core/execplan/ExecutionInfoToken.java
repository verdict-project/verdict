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

package org.verdictdb.core.execplan;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class ExecutionInfoToken implements Serializable {

  private static final long serialVersionUID = 4467660505348718275L;

  Map<String, Object> data = new HashMap<>();

  public static ExecutionInfoToken empty() {
    return new ExecutionInfoToken();
  }

  public boolean isStatusToken() {
    return data.containsKey("status");
  }

  public static ExecutionInfoToken successToken() {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("status", "success");
    return token;
  }

  public static ExecutionInfoToken failureToken() {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("status", "failed");
    return token;
  }

  public static ExecutionInfoToken failureToken(Exception e) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("status", "failed");
    token.setKeyValue("errorMessage", e);
    return token;
  }

  public boolean isSuccessToken() {
    if (data.containsKey("status") && data.get("status").equals("success")) {
      return true;
    } else {
      return false;
    }
  }

  public boolean isFailureToken() {
    if (data.containsKey("status") && data.get("status").equals("failed")) {
      return true;
    } else {
      return false;
    }
  }

  public Object getValue(String key) {
    return data.get(key);
  }

  public void setKeyValue(String key, Object value) {
    data.put(key, value);
  }

  //  public Map<String, Object> getData() {
  //    return data;
  //  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }

  public Iterable<Map.Entry<String, Object>> entrySet() {
    return data.entrySet();
  }

  public boolean containsKey(String key) {
    return data.containsKey(key);
  }

  public ExecutionInfoToken deepcopy() {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(bos);
      out.writeObject(this);
      out.flush();
      out.close();

      ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
      ExecutionInfoToken copiedToken = (ExecutionInfoToken) in.readObject();
      return copiedToken;

    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (NotSerializableException e) {
      // TODO: handle this case
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;

    //    ExecutionInfoToken newToken = new ExecutionInfoToken();
    //
    //    for (Entry<String, Object> entry : data.entrySet()) {
    //      String key = entry.getKey();
    //      Object value = entry.getValue();
    //
    //      try {
    //        ByteArrayOutputStream bos = new ByteArrayOutputStream();
    //        ObjectOutputStream out = new ObjectOutputStream(bos);
    //        out.writeObject(value);
    //        out.flush();
    //        out.close();
    //
    //        ObjectInputStream in = new ObjectInputStream(
    //            new ByteArrayInputStream(bos.toByteArray()));
    //        Object copedValue = in.readObject();
    //        newToken.setKeyValue(key, copedValue);
    //
    //      } catch (ClassNotFoundException | IOException e) {
    //        e.printStackTrace();
    //      }
    //    }
    //
    //    return newToken;
  }
}
