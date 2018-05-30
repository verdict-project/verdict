package org.verdictdb.core.sql;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;
import org.verdictdb.core.logical_query.AsteriskColumn;
import org.verdictdb.core.logical_query.BaseTable;
import org.verdictdb.core.logical_query.CreateTableAsSelect;
import org.verdictdb.core.logical_query.SelectItem;
import org.verdictdb.core.logical_query.SelectQueryOp;
import org.verdictdb.core.sql.syntax.HiveSyntax;
import org.verdictdb.exception.VerdictDbException;

public class CreateTableAsSelectToSqlTest {

  @Test
  public void selectAllTest() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
            Arrays.<SelectItem>asList(new AsteriskColumn()),
            base);
    CreateTableAsSelect create = new CreateTableAsSelect("newtable", relation);
    String expected = "create mytable as select * from `myschema`.`mytable` as t";
    LogicalSelectQueryToSql relToSql = new LogicalSelectQueryToSql(new HiveSyntax());
//    String actual = relToSql.toSql(create);
//    assertEquals(expected, actual);
  }

}
