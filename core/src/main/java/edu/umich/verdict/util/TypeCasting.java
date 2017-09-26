/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictJDBCContext;

public class TypeCasting {

    public static <T, S> Map<T, S> listToMap(List<Pair<T, S>> list) {
        Map<T, S> amap = new HashMap<T, S>();
        for (Pair<T, S> p : list) {
            amap.put(p.getKey(), p.getValue());
        }
        return amap;
    }

    public static <T, S> Map<T, S> listToReverseMap(List<Pair<S, T>> list) {
        Map<T, S> amap = new HashMap<T, S>();
        for (Pair<S, T> p : list) {
            amap.put(p.getValue(), p.getKey());
        }
        return amap;
    }

    public static <T, S> List<Pair<T, S>> mapToList(Map<T, S> map) {
        List<Pair<T, S>> l = new ArrayList<Pair<T, S>>();
        for (Map.Entry<T, S> e : map.entrySet()) {
            l.add(Pair.of(e.getKey(), e.getValue()));
        }
        return l;
    }

    public static <S, T> Map<S, T> reverseMap(Map<T, S> amap) {
        Map<S, T> newMap = new HashMap<S, T>();
        for (Map.Entry<T, S> e : amap.entrySet()) {
            newMap.put(e.getValue(), e.getKey());
        }
        return newMap;
    }

    public static Double toDouble(Object obj) {
        if (obj instanceof Double)
            return (Double) obj;
        else if (obj instanceof Float)
            return ((Float) obj).doubleValue();
        else if (obj instanceof BigDecimal)
            return ((BigDecimal) obj).doubleValue();
        else if (obj instanceof Long)
            return ((Long) obj).doubleValue();
        else if (obj instanceof Integer)
            return ((Integer) obj).doubleValue();
        else {
            VerdictLogger.error("Cannot convert to double, just return 0: " + obj.toString());
            return new Double(0);
        }
    }

    public static List<Double> toDoubleList(List<Object> ol) {
        List<Double> dl = new ArrayList<Double>();
        for (Object o : ol) {
            dl.add(toDouble(o));
        }
        return dl;
    }

    public static long toLong(Object obj) {
        if (obj instanceof Double)
            return Math.round((Double) obj);
        else if (obj instanceof Float)
            return Math.round((Float) obj);
        else if (obj instanceof BigDecimal)
            return ((BigDecimal) obj).toBigInteger().longValue();
        else if (obj instanceof Long)
            return ((Long) obj);
        else if (obj instanceof Integer)
            return ((Integer) obj);
        else {
            VerdictLogger.error("Cannot convert to long value, just returnning 0: " + obj.toString());
            return 0;
        }
    }

    public static List<Long> toLongList(List<Object> ol) {
        List<Long> ll = new ArrayList<Long>();
        for (Object o : ol) {
            ll.add(toLong(o));
        }
        return ll;
    }

    /**
     * 
     * @param type
     *            should be java.sql.Types
     * @return
     */
    public static boolean isTypeNumeric(int type) {
        if (type == java.sql.Types.BIGINT || type == java.sql.Types.DECIMAL || type == java.sql.Types.DOUBLE
                || type == java.sql.Types.FLOAT || type == java.sql.Types.INTEGER || type == java.sql.Types.NUMERIC
                || type == java.sql.Types.REAL || type == java.sql.Types.SMALLINT || type == java.sql.Types.TINYINT) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isObjectNumeric(Object o) {
        if (o instanceof Double || o instanceof Float || o instanceof Integer || o instanceof Long) {
            return true;
        } else {
            return false;
        }
    }

    private static final Map<Integer, String> typeInt2TypeName;
    static {
        Map<Integer, String> map = new HashMap<Integer, String>();
        map.put(java.sql.Types.BIGINT, "BIGINT");
        map.put(java.sql.Types.BOOLEAN, "BOOLEAN");
        map.put(java.sql.Types.CHAR, "CHAR");
        map.put(java.sql.Types.DATE, "DATE");
        map.put(java.sql.Types.DECIMAL, "DECIMAL");
        map.put(java.sql.Types.DOUBLE, "DOUBLE");
        map.put(java.sql.Types.FLOAT, "FLOAT");
        map.put(java.sql.Types.INTEGER, "INTEGER");
        map.put(java.sql.Types.NUMERIC, "NUMERIC");
        map.put(java.sql.Types.REAL, "REAL");
        map.put(java.sql.Types.SMALLINT, "SMALLINT");
        map.put(java.sql.Types.TIME, "TIME");
        map.put(java.sql.Types.TIMESTAMP, "TIMESTAMP");
        map.put(java.sql.Types.TINYINT, "TINYINT");
        map.put(java.sql.Types.VARCHAR, "VARCHAR");
        typeInt2TypeName = Collections.unmodifiableMap(map);
    }

    public static String dbDatatypeName(int type) {
        return typeInt2TypeName.get(type);
    }

    public static boolean isHiveFamily(String dbName) {
        return (dbName.equals("impala") || dbName.equals("hive")) ? true : false;
    }

    public static String dbDatatypeNameWithDefaultParam(VerdictJDBCContext vc, int type) {
        String dbName = vc.getDbms().getName();
        String typename = dbDatatypeName(type);

        if (type == java.sql.Types.VARCHAR) {
            if (isHiveFamily(dbName)) {
                return typename;
            } else {
                return typename + "(255)";
            }
        } else if (type == java.sql.Types.DECIMAL) {
            if (isHiveFamily(dbName)) {
                return typename;
            } else {
                return typename + "(10,5)";
            }
        } else if (type == java.sql.Types.NUMERIC) {
            if (isHiveFamily(dbName)) {
                return typename;
            } else {
                return typename + "(10,5)";
            }
        } else if (type == java.sql.Types.INTEGER) {
            if (isHiveFamily(dbName)) {
                return typename;
            } else {
                return typename + "(10)";
            }
        } else {
            return typename;
        }
    }

}
