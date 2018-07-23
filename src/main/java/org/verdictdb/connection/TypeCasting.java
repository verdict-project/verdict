/*
 *    Copyright 2017 University of Michigan
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

import java.math.BigDecimal;

public class TypeCasting {

  public static Double toDouble(Object obj) {
    if (obj instanceof Double) return (Double) obj;
    else if (obj instanceof Float) return ((Float) obj).doubleValue();
    else if (obj instanceof BigDecimal) return ((BigDecimal) obj).doubleValue();
    else if (obj instanceof Long) return ((Long) obj).doubleValue();
    else if (obj instanceof Integer) return ((Integer) obj).doubleValue();
    else if (obj instanceof Short) return ((Short) obj).doubleValue();
    else if (obj instanceof Byte) return ((Byte) obj).doubleValue();
    else {
      return null;
    }
  }

  public static Float toFloat(Object obj) {
    if (obj instanceof Double) return ((Double) obj).floatValue();
    else if (obj instanceof Float) return ((Float) obj);
    else if (obj instanceof BigDecimal) return ((BigDecimal) obj).floatValue();
    else if (obj instanceof Long) return ((Long) obj).floatValue();
    else if (obj instanceof Integer) return ((Integer) obj).floatValue();
    else if (obj instanceof Short) return ((Short) obj).floatValue();
    else if (obj instanceof Byte) return ((Byte) obj).floatValue();
    else {
      return null;
    }
  }

  public static BigDecimal toBigDecimal(Object obj) {
    if (obj instanceof Double) return new BigDecimal((Double) obj);
    else if (obj instanceof Float) return new BigDecimal((Float) obj);
    else if (obj instanceof BigDecimal) return ((BigDecimal) obj);
    else if (obj instanceof Long) return new BigDecimal((Long) obj);
    else if (obj instanceof Integer) return new BigDecimal((Integer) obj);
    else if (obj instanceof Short) return new BigDecimal((Short) obj);
    else if (obj instanceof Byte) return new BigDecimal((Byte) obj);
    else {
      throw null;
    }
  }

  public static BigDecimal toBigDecimal(Object obj, int scale) {
    if (obj instanceof Double) return new BigDecimal((Double) obj).setScale(scale);
    else if (obj instanceof Float) return new BigDecimal((Float) obj).setScale(scale);
    else if (obj instanceof BigDecimal) return ((BigDecimal) obj).setScale(scale);
    else if (obj instanceof Long) return new BigDecimal((Long) obj).setScale(scale);
    else if (obj instanceof Integer) return new BigDecimal((Integer) obj).setScale(scale);
    else if (obj instanceof Short) return new BigDecimal((Short) obj).setScale(scale);
    else if (obj instanceof Byte) return new BigDecimal((Byte) obj).setScale(scale);
    else {
      throw null;
    }
  }

  public static Long toLong(Object obj) {
    if (obj instanceof Double) return ((Double) obj).longValue();
    else if (obj instanceof Float) return ((Float) obj).longValue();
    else if (obj instanceof BigDecimal) return ((BigDecimal) obj).toBigInteger().longValue();
    else if (obj instanceof Long) return ((Long) obj);
    else if (obj instanceof Integer) return ((Integer) obj).longValue();
    else if (obj instanceof Short) return ((Short) obj).longValue();
    else if (obj instanceof Byte) return ((Byte) obj).longValue();
    else if (obj instanceof Boolean) return ((Boolean) obj) ? (long) 1 : (long) 0;
    else {
      return null;
      //      throw new VerdictDBTypeException(obj);
    }
  }

  public static Integer toInteger(Object obj) {
    if (obj instanceof Double) return ((Double) obj).intValue();
    else if (obj instanceof Float) return ((Float) obj).intValue();
    else if (obj instanceof BigDecimal) return ((BigDecimal) obj).toBigInteger().intValue();
    else if (obj instanceof Long) return ((Long) obj).intValue();
    else if (obj instanceof Integer) return ((Integer) obj);
    else if (obj instanceof Short) return ((Short) obj).intValue();
    else if (obj instanceof Byte) return ((Byte) obj).intValue();
    else if (obj instanceof Boolean) return ((Boolean) obj) ? 1 : 0;
    else {
      return null;
      //      throw new VerdictDBTypeException(obj);
    }
  }

  public static Short toShort(Object obj) {
    if (obj instanceof Double) return ((Double) obj).shortValue();
    else if (obj instanceof Float) return ((Float) obj).shortValue();
    else if (obj instanceof BigDecimal) return ((BigDecimal) obj).toBigInteger().shortValue();
    else if (obj instanceof Long) return ((Long) obj).shortValue();
    else if (obj instanceof Integer) return ((Integer) obj).shortValue();
    else if (obj instanceof Short) return ((Short) obj);
    else if (obj instanceof Byte) return ((Byte) obj).shortValue();
    else if (obj instanceof Boolean) return ((Boolean) obj) ? (short) 1 : (short) 0;
    else {
      return null;
      //      throw new VerdictDBTypeException(obj);
    }
  }

  public static Byte toByte(Object obj) {
    if (obj instanceof Double) return ((Double) obj).byteValue();
    else if (obj instanceof Float) return ((Float) obj).byteValue();
    else if (obj instanceof BigDecimal) return ((BigDecimal) obj).toBigInteger().byteValue();
    else if (obj instanceof Long) return ((Long) obj).byteValue();
    else if (obj instanceof Integer) return ((Integer) obj).byteValue();
    else if (obj instanceof Short) return ((Short) obj).byteValue();
    else if (obj instanceof Byte) return ((Byte) obj);
    else if (obj instanceof Boolean) return ((Boolean) obj) ? (byte) 1 : (byte) 0;
    else {
      return null;
      //      throw new VerdictDBTypeException(obj);
    }
  }
}
