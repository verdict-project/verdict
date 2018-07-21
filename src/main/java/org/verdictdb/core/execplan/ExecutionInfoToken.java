package org.verdictdb.core.execplan;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.rits.cloning.Cloner;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.postgresql.core.PGStream;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.jdbc.PgSQLXML;
import org.verdictdb.commons.UUIDSerializer;
import org.verdictdb.connection.JdbcQueryResult;

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
    return new Cloner().deepClone(this);
  }

}
