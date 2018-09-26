package org.verdictdb.commons;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;

import com.google.common.collect.Sets;

public class StringSplitter {

  /**
   * Splits a given query using the delimiter. The delimiters in quote chars are ignored.
   *
   * <p>Note: I have tried many regex-based and the Apache commons library for this, but they do not
   * work. Regex throws StackOverflowError, and the StringTokenizer by the commons library is
   * incorrect for our purpose.
   *
   * @param sql
   */
  public static List<String> splitOnSemicolon(String sql, String quoteChars) {
    List<String> splitted = new ArrayList<>();
    Map<Character, Integer> quoteCharCounts = new HashMap<>();
    Set<Character> quoteCharSet = Sets.newHashSet(ArrayUtils.toObject(quoteChars.toCharArray()));
    for (char c : quoteCharSet) {
      quoteCharCounts.put(c, 0);
    }
    char delimiter = ';';

    StringBuilder beginConstructed = new StringBuilder();
    for (char c : sql.toCharArray()) {
      // when encountered a delimiter
      if (c == delimiter) {
        // if there is no odd-count quote chars, we create a new sql
        boolean oddCountQuoteExist = false;
        for (int count : quoteCharCounts.values()) {
          if (count % 2 == 1) {
            oddCountQuoteExist = true;
            break;
          }
        }
        if (oddCountQuoteExist == false) {
          // create a new sql
          splitted.add(beginConstructed.toString());
          beginConstructed = new StringBuilder();
          ;
        }
      } else {
        beginConstructed.append(c);
        if (quoteCharSet.contains(c)) {
          quoteCharCounts.put(c, quoteCharCounts.get(c) + 1);
        }
      }
    }
    // if there anything remaining, add it as a separate sql
    if (beginConstructed.length() > 0) {
      String s = beginConstructed.toString();
      if (s.trim().length() > 0) {
        splitted.add(s);
      }
    }

    return splitted;
  }

}
