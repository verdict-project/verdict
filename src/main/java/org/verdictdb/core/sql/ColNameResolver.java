package org.verdictdb.core.sql;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.logical_query.*;
import org.verdictdb.parser.VerdictSQLBaseVisitor;
import org.verdictdb.parser.VerdictSQLParser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class ColNameResolver extends VerdictSQLBaseVisitor<UnnamedColumn> {

  private HashMap<String, String> tableAliasAndColNames;

  private HashMap<Pair<String, String>, String> tableInfoAndAlias;

  private MetaData meta;

  public ColNameResolver(HashMap<String, String> tableAliasAndColNames, HashMap<Pair<String, String>, String> tableInfoAndAlias, MetaData meta) {
    this.tableAliasAndColNames = tableAliasAndColNames;
    this.tableInfoAndAlias = tableInfoAndAlias;
    this.meta = meta;
  }


  @Override
  public BaseColumn visitColumn_ref_expression(VerdictSQLParser.Column_ref_expressionContext ctx) {
    String[] t = ctx.getText().split("\\.");
    if (t.length == 3) {
      return new BaseColumn(tableInfoAndAlias.get(new ImmutablePair<>(t[0], t[1])), t[2]);
    } else if (t.length == 2) {
      return new BaseColumn(tableAliasAndColNames.get(t[1]), t[1]);
    } else {
      return new BaseColumn(tableAliasAndColNames.get(t[1]), t[1]);
    }
  }
}
