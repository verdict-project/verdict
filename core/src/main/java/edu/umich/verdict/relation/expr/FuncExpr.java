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

package edu.umich.verdict.relation.expr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.util.StringManipulations;

public class FuncExpr extends Expr {

    public enum FuncName {
        COUNT, SUM, AVG, COUNT_DISTINCT, EXTRACT, IMPALA_APPROX_COUNT_DISTINCT, ROUND, MAX, MIN, FLOOR,
        CEIL, EXP, LN, LOG10, LOG2, SIN, COS, TAN, SIGN, STRTOL, RAND, RANDOM, FNV_HASH, ABS, STDDEV,
        SQRT, MOD, PMOD, YEAR, QUARTER, MONTH, DAY, HOUR, MINUTE, SECOND, WEEKOFYEAR, CAST, CONV, SUBSTR,
        MD5, CRC32, UNIX_TIMESTAMP, CURRENT_TIMESTAMP, UNKNOWN, LOWER, UPPER, ASCII, CHARACTER_LENGTH,
        POW, E, PI, FACTORIAL, CBRT, PERCENTILE, SPLIT, LENGTH, INSTR, TRIM, ASIN, ACOS, ATAN, DEGREES,
        RADIANS, POSITIVE, NEGATIVE, ENCODE, DECODE, BROUND, BIN, HEX, UNHEX, SHIFTLEFT, SHIFTRIGHT,
        SHIFTRIGHTUNSIGNED, FROM_UNIXTIME, TO_DATE, NVL, CHR, FIND_IN_SET, FORMAT_NUMBER, GET_JSON_OBJECT,
        IN_FILE, LOCATE, LTRIM, REPEAT, REVERSE, SPACE, AES_ENCRYPT, AES_DECRYPT, SHA1, SHA2, STDDEV_SAMP,
        CONCAT, CONCAT_WS, RANK, DENSE_RANK, NTILE, ROW_NUMBER, COALESCE, HASH, RPAD, RAWTOHEX
    }

    public static final ImmutableList<FuncName> AGG_FUNC_LIST =
            ImmutableList.of(FuncName.COUNT, FuncName.SUM, FuncName.AVG, FuncName.COUNT_DISTINCT,
                    FuncName.IMPALA_APPROX_COUNT_DISTINCT);

    protected List<Expr> expressions;

    protected FuncName funcname;

    protected OverClause overClause;

    protected static Map<String, FuncName> string2FunctionType =
            ImmutableMap.<String, FuncName>builder()
            .put("ABS", FuncName.ABS)
            .put("ACOS", FuncName.ACOS)
            .put("AES_DECRYPT", FuncName.AES_DECRYPT)
            .put("AES_ENCRYPT", FuncName.AES_ENCRYPT)
            .put("ASCII", FuncName.ASCII)
            .put("ASIN", FuncName.ASIN)
            .put("ATAN", FuncName.ATAN)
            .put("BIN", FuncName.BIN)
            .put("BROUND", FuncName.BROUND)
            .put("COALESCE", FuncName.COALESCE)
            .put("CAST", FuncName.CAST)
            .put("CBRT", FuncName.CBRT)
            .put("CEIL", FuncName.CEIL)
            .put("CHARACTER_LENGTH", FuncName.CHARACTER_LENGTH)
            .put("CHR", FuncName.CHR)
            .put("CONV", FuncName.CONV)
            .put("CONCAT", FuncName.CONCAT)
            .put("CONCAT_WS", FuncName.CONCAT_WS)
            .put("COS", FuncName.COS)
            .put("CRC32", FuncName.CRC32)
            .put("CURRENT_TIMESTAMP", FuncName.CURRENT_TIMESTAMP)
            .put("DAY", FuncName.DAY)
            .put("DECODE", FuncName.DECODE)
            .put("DEGREES", FuncName.DEGREES)
            .put("E", FuncName.E)
            .put("ENCODE", FuncName.ENCODE)
            .put("EXP", FuncName.EXP)
            .put("EXTRACT", FuncName.EXTRACT)
            .put("FACTORIAL", FuncName.FACTORIAL)
            .put("FIND_IN_SET", FuncName.FIND_IN_SET)
            .put("FLOOR", FuncName.FLOOR)
            .put("FNV_HASH", FuncName.FNV_HASH)
            .put("FORMAT_NUMBER", FuncName.FORMAT_NUMBER)
            .put("FROM_UNIXTIME", FuncName.FROM_UNIXTIME)
            .put("GET_JSON_OBJECT", FuncName.GET_JSON_OBJECT)
            .put("HASH", FuncName.HASH)
            .put("HEX", FuncName.HEX)
            .put("HOUR", FuncName.HOUR)
            .put("INSTR", FuncName.INSTR)
            .put("IN_FILE", FuncName.IN_FILE)
            .put("LENGTH", FuncName.LENGTH)
            .put("LN", FuncName.LN)
            .put("LOCATE", FuncName.LOCATE)
            .put("LOG10", FuncName.LOG10)
            .put("LOG2", FuncName.LOG2)
            .put("LOWER", FuncName.LOWER)
            .put("LTRIM", FuncName.LTRIM)
            .put("MAX", FuncName.MAX)
            .put("MD5", FuncName.MD5)
            .put("MIN", FuncName.MIN)
            .put("MINUTE", FuncName.MINUTE)
            .put("MOD", FuncName.MOD)
            .put("MONTH", FuncName.MONTH)
            .put("NEGATIVE", FuncName.NEGATIVE)
            .put("NVL", FuncName.NVL)
            .put("PERCENTILE", FuncName.PERCENTILE)
            .put("PI", FuncName.PI)
            .put("PMOD", FuncName.PMOD)
            .put("POSITIVE", FuncName.POSITIVE)
            .put("POW", FuncName.POW)
            .put("QUARTER", FuncName.QUARTER)
            .put("RADIANS", FuncName.RADIANS)
            .put("RAND", FuncName.RAND)
            .put("RANDOM", FuncName.RANDOM)
            .put("RAWTOHEX", FuncName.RAWTOHEX)
            .put("REPEAT", FuncName.REPEAT)
            .put("REVERSE", FuncName.REVERSE)
            .put("ROUND", FuncName.ROUND)
            .put("RPAD", FuncName.RPAD)
            .put("SECOND", FuncName.SECOND)
            .put("SHA1", FuncName.SHA1)
            .put("SHA2", FuncName.SHA2)
            .put("SHIFTLEFT", FuncName.SHIFTLEFT)
            .put("SHIFTRIGHT", FuncName.SHIFTRIGHT)
            .put("SHIFTRIGHTUNSIGNED", FuncName.SHIFTRIGHTUNSIGNED)
            .put("SIGN", FuncName.SIGN)
            .put("SIN", FuncName.SIN)
            .put("SPACE", FuncName.SPACE)
            .put("SPLIT", FuncName.SPLIT)
            .put("SQRT", FuncName.SQRT)
            .put("STDDEV", FuncName.STDDEV)
            .put("STDDEV_SAMP", FuncName.STDDEV_SAMP)
            .put("STRTOL", FuncName.STRTOL)
            .put("SUBSTR", FuncName.SUBSTR)
            .put("TAN", FuncName.TAN)
            .put("TO_DATE", FuncName.TO_DATE)
            .put("TRIM", FuncName.TRIM)
            .put("UNHEX", FuncName.UNHEX)
            .put("UNIX_TIMESTAMP", FuncName.UNIX_TIMESTAMP)
            .put("UPPER", FuncName.UPPER)
            .put("WEEKOFYEAR", FuncName.WEEKOFYEAR)
            .put("YEAR", FuncName.YEAR)
            .put("RANK", FuncName.RANK)
            .put("DENSE_RANK", FuncName.DENSE_RANK)
            .put("NTILE", FuncName.NTILE)
            .put("ROW_NUMBER", FuncName.ROW_NUMBER)
            .build();

    protected static Map<FuncName, String> functionPattern = ImmutableMap.<FuncName, String>builder()
            .put(FuncName.ABS, "abs(%s)")
            .put(FuncName.ACOS, "acos(%s)")
            .put(FuncName.AES_DECRYPT, "aes_decrypt(%s, %s)")
            .put(FuncName.AES_ENCRYPT, "aes_encrypt(%s, %s)")
            .put(FuncName.ASCII, "ascii(%s)")
            .put(FuncName.ASIN, "asin(%s)")
            .put(FuncName.ATAN, "atan(%s)")
            .put(FuncName.AVG, "avg(%s)")
            .put(FuncName.BIN, "bin(%s)")
            .put(FuncName.BROUND, "bround(%s)")
            .put(FuncName.COALESCE, "coalesce(%s)")
            .put(FuncName.CAST, "cast(%s as %s)")
            .put(FuncName.CBRT, "cbrt(%s)")
            .put(FuncName.CEIL, "ceil(%s)")
            .put(FuncName.CHARACTER_LENGTH, "character_length(%s)")
            .put(FuncName.CHR, "chr(%s)")
            .put(FuncName.CONCAT, "concat(%s)")
            .put(FuncName.CONCAT_WS, "concat_ws(%s)")
            .put(FuncName.CONV, "conv(%s, %s, %s)")
            .put(FuncName.COS, "cos(%s)")
            .put(FuncName.COUNT, "count(%s)")
            .put(FuncName.COUNT_DISTINCT, "count(distinct %s)")
            .put(FuncName.CRC32, "crc32(%s)")
            .put(FuncName.CURRENT_TIMESTAMP, "current_timestamp(%s)")
            .put(FuncName.DAY, "day(%s)")
            .put(FuncName.DECODE, "decode(%s, %s)")
            .put(FuncName.DEGREES, "degrees(%s)")
            .put(FuncName.E, "e(%s)")
            .put(FuncName.ENCODE, "encode(%s, %s)")
            .put(FuncName.EXP, "exp(%s)")
            .put(FuncName.EXTRACT, "extract(%s from %s)")
            .put(FuncName.FACTORIAL, "factorial(%s)")
            .put(FuncName.FIND_IN_SET, "find_in_set(%s, %s)")
            .put(FuncName.FLOOR, "floor(%s)")
            .put(FuncName.FNV_HASH, "fnv_hash(%s)")
            .put(FuncName.FORMAT_NUMBER, "format_number(%s, %s)")
            .put(FuncName.FROM_UNIXTIME, "from_unixtime(%s)")
            .put(FuncName.GET_JSON_OBJECT, "get_json_object(%s, %s)")
            .put(FuncName.HASH, "hash(%s, %s, %s)")
            .put(FuncName.HEX, "hex(%s)")
            .put(FuncName.HOUR, "hour(%s)")
            .put(FuncName.IMPALA_APPROX_COUNT_DISTINCT, "ndv(%s)")
            .put(FuncName.INSTR, "instr(%s, %s)")
            .put(FuncName.IN_FILE, "in_file(%s, %s)")
            .put(FuncName.LENGTH, "length(%s)")
            .put(FuncName.LN, "ln(%s)")
            .put(FuncName.LOCATE, "locate(%s, %s)")
            .put(FuncName.LOG10, "log10(%s)")
            .put(FuncName.LOG2, "log2(%s)")
            .put(FuncName.LOWER, "lower(%s)")
            .put(FuncName.LTRIM, "ltrim(%s)")
            .put(FuncName.MAX, "max(%s)")
            .put(FuncName.MD5, "md5(%s)")
            .put(FuncName.MIN, "min(%s)")
            .put(FuncName.MINUTE, "minute(%s)")
            .put(FuncName.MOD, "mod(%s, %s)")
            .put(FuncName.MONTH, "month(%s)")
            .put(FuncName.NEGATIVE, "negative(%s)")
            .put(FuncName.NVL, "nvl(%s, %s)")
            .put(FuncName.PERCENTILE, "percentile(%s, %s)")
            .put(FuncName.PI, "pi(%s)")
            .put(FuncName.PMOD, "pmod(%s, %s)")
            .put(FuncName.POSITIVE, "positive(%s)")
            .put(FuncName.POW, "pow(%s, %s)")
            .put(FuncName.QUARTER, "quarter(%s)")
            .put(FuncName.RADIANS, "radians(%s)")
            .put(FuncName.RAND, "rand(%s)")
            .put(FuncName.RANDOM, "random(%s)")
            .put(FuncName.RAWTOHEX, "rawtohex(%s)")
            .put(FuncName.REPEAT, "repeat(%s, %s)")
            .put(FuncName.REVERSE, "reverse(%s)")
            .put(FuncName.ROUND, "round(%s)")
            .put(FuncName.RPAD, "rpad(%s, %s, %s)")
            .put(FuncName.SECOND, "second(%s)")
            .put(FuncName.SHA1, "sha1(%s)")
            .put(FuncName.SHA2, "sha2(%s)")
            .put(FuncName.SHIFTLEFT, "shiftleft(%s, %s)")
            .put(FuncName.SHIFTRIGHT, "shiftright(%s, %s)")
            .put(FuncName.SHIFTRIGHTUNSIGNED, "shiftrightunsigned(%s, %s)")
            .put(FuncName.SIGN, "sign(%s)")
            .put(FuncName.SIN, "sin(%s)")
            .put(FuncName.SPACE, "space(%s)")
            .put(FuncName.SPLIT, "split(%s, %s)")
            .put(FuncName.SQRT, "sqrt(%s)")
            .put(FuncName.STDDEV, "stddev(%s)")
            .put(FuncName.STDDEV_SAMP, "stddev_samp(%s)")
            .put(FuncName.STRTOL, "strtol(%s, %s)")
            .put(FuncName.SUBSTR, "substr(%s, %s, %s)")
            .put(FuncName.SUM, "sum(%s)")
            .put(FuncName.TAN, "tan(%s)")
            .put(FuncName.TO_DATE, "to_date(%s)")
            .put(FuncName.TRIM, "trim(%s)")
            .put(FuncName.UNHEX, "unhex(%s)")
            .put(FuncName.UNIX_TIMESTAMP, "unix_timestamp(%s)")
            .put(FuncName.UNKNOWN, "UNKNOWN(%s)")
            .put(FuncName.UPPER, "upper(%s)")
            .put(FuncName.WEEKOFYEAR, "weekofyear(%s)")
            .put(FuncName.YEAR, "year(%s)")
            .put(FuncName.RANK, "rank(%s)")
            .put(FuncName.DENSE_RANK, "dense_rank(%s)")
            .put(FuncName.NTILE, "ntile(%s)")
            .put(FuncName.ROW_NUMBER, "row_number(%s)")
            .build();

    public FuncExpr(FuncName fname, List<Expr> exprs, OverClause overClause) {
        super(VerdictContext.dummyContext());
        this.expressions = exprs;
        this.funcname = fname;
        this.overClause = overClause;
    }

    public FuncExpr(FuncName fname, Expr expr1, Expr expr2, Expr expr3, OverClause overClause) {
        super(VerdictContext.dummyContext());
        this.expressions = new ArrayList<Expr>();
        if (expr1 != null) {
            this.expressions.add(expr1);
            if (expr2 != null) {
                this.expressions.add(expr2);
                if (expr3 != null) {
                    this.expressions.add(expr3);
                }
            }
        }
        this.funcname = fname;
        this.overClause = overClause;
    }

    public FuncExpr(FuncName fname, Expr expr1, Expr expr2, OverClause overClause) {
        this(fname, expr1, expr2, null, overClause);
    }

    public FuncExpr(FuncName fname, Expr expr, OverClause overClause) {
        this(fname, expr, null, overClause);
    }

    public FuncExpr(FuncName fname, Expr expr1, Expr expr2, Expr expr3) {
        this(fname, expr1, expr2, expr3, null);
    }

    public FuncExpr(FuncName fname, Expr expr1, Expr expr2) {
        this(fname, expr1, expr2, null, null);
    }

    public FuncExpr(FuncName fname, Expr expr) {
        this(fname, expr, null, null, null);
    }

    public List<Expr> getExpressions() {
        return expressions;
    }

    public FuncName getFuncName() {
        return funcname;
    }

    public OverClause getOverClause() {
        return overClause;
    }

    public static FuncExpr from(VerdictContext vc, String expr) {
        VerdictSQLParser p = StringManipulations.parserOf(expr);
        return from(vc, p.function_call());
    }

    public static FuncExpr from(final VerdictContext vc, VerdictSQLParser.Function_callContext ctx) {
        VerdictSQLBaseVisitor<FuncExpr> v = new VerdictSQLBaseVisitor<FuncExpr>() {

            @Override
            public FuncExpr visitRanking_windowed_function(VerdictSQLParser.Ranking_windowed_functionContext ctx) {
                FuncName fname;
                Expr expr = null;
                OverClause overClause = null;
                if (ctx.RANK() != null) {
                    fname = FuncName.RANK;
                } else if (ctx.DENSE_RANK() != null) {
                    fname = FuncName.DENSE_RANK;
                } else if (ctx.NTILE() != null) {
                    fname = FuncName.NTILE;
                } else if (ctx.ROW_NUMBER() != null) {
                    fname = FuncName.ROW_NUMBER;
                } else {
                    fname = FuncName.UNKNOWN;
                }
                if (ctx.over_clause() != null) {
                    overClause = OverClause.from(vc, ctx.over_clause());
                }

                return new FuncExpr(fname, expr, overClause);
            }

            @Override
            public FuncExpr visitAggregate_windowed_function(VerdictSQLParser.Aggregate_windowed_functionContext ctx) {
                FuncName fname;
                Expr expr = null;
                if (ctx.all_distinct_expression() != null) {
                    expr = Expr.from(vc, ctx.all_distinct_expression().expression());
                }
                OverClause overClause = null;

                if (ctx.AVG() != null) {
                    fname = FuncName.AVG;
                } else if (ctx.SUM() != null) {
                    fname = FuncName.SUM;
                } else if (ctx.COUNT() != null) {
                    if (ctx.all_distinct_expression() != null && ctx.all_distinct_expression().DISTINCT() != null) {
                        fname = FuncName.COUNT_DISTINCT;
                    } else {
                        fname = FuncName.COUNT;
                        expr = new StarExpr();
                    }
                } else if (ctx.NDV() != null) {
                    fname = FuncName.IMPALA_APPROX_COUNT_DISTINCT;
                } else if (ctx.MIN() != null) {
                    fname = FuncName.MIN;
                } else if (ctx.MAX() != null) {
                    fname = FuncName.MAX;
                } else if (ctx.STDDEV_SAMP() != null) {
                    fname = FuncName.STDDEV_SAMP;
                } else {
                    fname = FuncName.UNKNOWN;
                }

                if (ctx.over_clause() != null) {
                    overClause = OverClause.from(vc, ctx.over_clause());
                }

                return new FuncExpr(fname, expr, overClause);
            }

            @Override
            public FuncExpr visitUnary_manipulation_function(VerdictSQLParser.Unary_manipulation_functionContext ctx) {
                String fname = ctx.function_name.getText().toUpperCase();
                FuncName funcName = string2FunctionType.containsKey(fname) ? string2FunctionType.get(fname)
                        : FuncName.UNKNOWN;
                if (fname.equals("CAST")) {
                    return new FuncExpr(funcName, Expr.from(vc, ctx.cast_as_expression().expression()),
                            ConstantExpr.from(vc, ctx.cast_as_expression().data_type().getText()));
                } else {
                    return new FuncExpr(funcName, Expr.from(vc, ctx.expression()));
                }
            }

            @Override
            public FuncExpr visitNoparam_manipulation_function(
                    VerdictSQLParser.Noparam_manipulation_functionContext ctx) {
                String fname = ctx.function_name.getText().toUpperCase();
                FuncName funcName = string2FunctionType.containsKey(fname) ? string2FunctionType.get(fname)
                        : FuncName.UNKNOWN;
                return new FuncExpr(funcName, null);
            }

            @Override
            public FuncExpr visitBinary_manipulation_function(
                    VerdictSQLParser.Binary_manipulation_functionContext ctx) {
                String fname = ctx.function_name.getText().toUpperCase();
                FuncName funcName = string2FunctionType.containsKey(fname) ? string2FunctionType.get(fname)
                        : FuncName.UNKNOWN;
                return new FuncExpr(funcName, Expr.from(vc, ctx.expression(0)), Expr.from(vc, ctx.expression(1)));
            }

            @Override
            public FuncExpr visitTernary_manipulation_function(
                    VerdictSQLParser.Ternary_manipulation_functionContext ctx) {
                String fname = ctx.function_name.getText().toUpperCase();
                FuncName funcName = string2FunctionType.containsKey(fname) ? string2FunctionType.get(fname)
                        : FuncName.UNKNOWN;
                return new FuncExpr(funcName, Expr.from(vc, ctx.expression(0)), Expr.from(vc, ctx.expression(1)),
                        Expr.from(vc, ctx.expression(2)));
            }

            @Override
            public FuncExpr visitNary_manipulation_function(VerdictSQLParser.Nary_manipulation_functionContext ctx) {
                String fname = ctx.function_name.getText().toUpperCase();
                FuncName funcName = string2FunctionType.containsKey(fname) ? string2FunctionType.get(fname)
                    : FuncName.UNKNOWN;
                List<Expr> exprList = new ArrayList<>();
                for (VerdictSQLParser.ExpressionContext context : ctx.expression()) {
                    exprList.add(Expr.from(vc, context));
                }
                return new FuncExpr(funcName, exprList, null);
            }

            @Override
            public FuncExpr visitExtract_time_function(VerdictSQLParser.Extract_time_functionContext ctx) {
                String fname = ctx.function_name.getText().toUpperCase();
                FuncName funcName = string2FunctionType.containsKey(fname) ? string2FunctionType.get(fname)
                        : FuncName.UNKNOWN;
                return new FuncExpr(funcName, ConstantExpr.from(vc, ctx.extract_unit()),
                        Expr.from(vc, ctx.expression()));
            }

        };
        return v.visit(ctx);
    }

    public Expr getUnaryExpr() {
        return expressions.get(0);
    }

    public String getUnaryExprInString() {
        return getUnaryExpr().toString();
    }

    public static FuncExpr avg(Expr expr) {
        return new FuncExpr(FuncName.AVG, expr);
    }

    public static FuncExpr avg(VerdictContext vc, String expr) {
        return avg(Expr.from(vc, expr));
    }

    public static FuncExpr sum(Expr expr) {
        return new FuncExpr(FuncName.SUM, expr);
    }

    public static FuncExpr sum(VerdictContext vc, String expr) {
        return sum(Expr.from(vc, expr));
    }

    public static FuncExpr count() {
        return new FuncExpr(FuncName.COUNT, new StarExpr());
    }

    public static FuncExpr countDistinct(Expr expr) {
        return new FuncExpr(FuncName.COUNT_DISTINCT, expr);
    }

    public static FuncExpr countDistinct(VerdictContext vc, String expr) {
        return countDistinct(Expr.from(vc, expr));
    }

    public static FuncExpr round(Expr expr) {
        return new FuncExpr(FuncName.ROUND, expr);
    }

    public static FuncExpr min(Expr expr) {
        return new FuncExpr(FuncName.MIN, expr);
    }

    public static FuncExpr max(Expr expr) {
        return new FuncExpr(FuncName.MAX, expr);
    }

    public static FuncExpr stddev(Expr expr) {
        return new FuncExpr(FuncName.STDDEV, expr);
    }

    public static FuncExpr sqrt(Expr expr) {
        return new FuncExpr(FuncName.SQRT, expr);
    }

    public static FuncExpr approxCountDistinct(VerdictContext vc, Expr expr) {
        if (vc.getDbms().getName().equalsIgnoreCase("impala")) {
            return new FuncExpr(FuncName.IMPALA_APPROX_COUNT_DISTINCT, expr);
        } else {
            return new FuncExpr(FuncName.COUNT_DISTINCT, expr);
        }
    }

    public static FuncExpr approxCountDistinct(VerdictContext vc, String expr) {
        return approxCountDistinct(vc, Expr.from(vc, expr));
    }

    @Override
    public String toString() {
        StringBuilder sql = new StringBuilder(50);
        if (funcname == FuncName.CONCAT || funcname == FuncName.CONCAT_WS ||
                funcname == FuncName.COALESCE) {
            Joiner joiner = Joiner.on(",");
            List<String> exprStrList = new ArrayList<>();
            for (Expr e : expressions) {
                exprStrList.add(e.toString());
            }
            String expr = joiner.join(exprStrList);
            sql.append(String.format(functionPattern.get(funcname), expr));
        } else if (expressions.size() == 0) {
            sql.append(String.format(functionPattern.get(funcname), ""));
        } else if (expressions.size() == 1) {
            sql.append(String.format(functionPattern.get(funcname), expressions.get(0).toString()));
        } else if (expressions.size() == 2) {
            sql.append(String.format(functionPattern.get(funcname), expressions.get(0).toString(),
                    expressions.get(1).toString()));
        } else {
            sql.append(String.format(functionPattern.get(funcname), expressions.get(0).toString(),
                    expressions.get(1).toString(), expressions.get(2).toString()));
        }

        if (overClause != null) {
            sql.append(" " + overClause.toString());
        }
        return sql.toString();
    }

    @Override
    public <T> T accept(ExprVisitor<T> v) {
        return v.call(this);
    }

    @Override
    public boolean isagg() {
        if (funcname.equals(FuncName.AVG) || funcname.equals(FuncName.SUM) || funcname.equals(FuncName.COUNT)
                || funcname.equals(FuncName.COUNT_DISTINCT) || funcname.equals(FuncName.IMPALA_APPROX_COUNT_DISTINCT)
                || funcname.equals(FuncName.MIN) || funcname.equals(FuncName.MAX)) {
            return true;
        } else {
            for (Expr expr : expressions) {
                if (expr.isagg())
                    return true;
            }
        }
        return false;
    }

    @Override
    public boolean isMeanLikeAggregate() {
        if (funcname.equals(FuncName.AVG) || funcname.equals(FuncName.SUM) || funcname.equals(FuncName.COUNT)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean isCountDistinct() {
        if (funcname.equals(FuncName.COUNT_DISTINCT)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean isCount() {
        if (funcname.equals(FuncName.COUNT)) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isMax() {
        if (funcname.equals(FuncName.MAX)) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isMin() {
        if (funcname.equals(FuncName.MIN)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Expr withTableSubstituted(String newTab) {
        List<Expr> newExprs = new ArrayList<Expr>();
        for (Expr e : expressions) {
            newExprs.add(e.withTableSubstituted(newTab));
        }
        return new FuncExpr(funcname, newExprs, overClause);
    }

    @Override
    public Expr withNewTablePrefix(String newPrefix) {
        List<Expr> newExprs = new ArrayList<Expr>();
        for (Expr e : expressions) {
            newExprs.add(e.withNewTablePrefix(newPrefix));
        }
        return new FuncExpr(funcname, newExprs, overClause);
    }

    @Override
    public String toSql() {
        StringBuilder sql = new StringBuilder(50);
        if (funcname == FuncName.CONCAT || funcname == FuncName.CONCAT_WS ||
                funcname == FuncName.COALESCE) {
            Joiner joiner = Joiner.on(",");
            List<String> exprStrList = new ArrayList<>();
            for (Expr e : expressions) {
                exprStrList.add(e.toString());
            }
            String expr = joiner.join(exprStrList);
            sql.append(String.format(functionPattern.get(funcname), expr));
        } else if (expressions.size() == 0) {
            sql.append(String.format(functionPattern.get(funcname), ""));
        } else if (expressions.size() == 1) {
            sql.append(String.format(functionPattern.get(funcname), expressions.get(0).toSql()));
        } else if (expressions.size() == 2) {
            sql.append(String.format(functionPattern.get(funcname), expressions.get(0).toSql(),
                    expressions.get(1).toSql()));
        } else {
            sql.append(String.format(functionPattern.get(funcname), expressions.get(0).toSql(),
                    expressions.get(1).toSql(), expressions.get(2).toSql()));
        }

        if (overClause != null) {
            sql.append(" " + overClause.toString());
        }
        return sql.toString();
    }

    @Override
    public int hashCode() {
        int s = 0;
        for (Expr e : getExpressions()) {
            s += e.hashCode();
        }
        s += funcname.hashCode();
        if (overClause == null)
            s += 1;
        return s;
    }

    @Override
    public boolean equals(Expr o) {
        if (o instanceof FuncExpr) {
            return getExpressions().equals(((FuncExpr) o).getExpressions())
                    && getFuncName().equals(((FuncExpr) o).getFuncName());
        }
        return false;
    }
}
