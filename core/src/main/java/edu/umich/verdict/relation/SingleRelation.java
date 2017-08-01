package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.ExprVisitor;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.util.VerdictLogger;

public class SingleRelation extends ExactRelation {

    protected TableUniqueName tableName;

    protected static final String NOSAMPLE = "nosample";

    public SingleRelation(VerdictContext vc, TableUniqueName tableName) {
        super(vc);
        this.tableName = tableName;
        this.subquery = false;
    }

    public static SingleRelation from(VerdictContext vc, TableUniqueName tableName) {
        SingleRelation r = new SingleRelation(vc, tableName);
        return r;
    }

    public static SingleRelation from(VerdictContext vc, String tableName) {
        SingleRelation r = new SingleRelation(vc, TableUniqueName.uname(vc, tableName));
        return r;
    }

    public TableUniqueName getTableName() {
        return tableName;
    }

    @Override
    public String toSql() {
        return select("*").toSql();
    }

    @Override
    protected String getSourceName() {
        return (alias == null)? tableName.getTableName() : getAlias();
    }

    /*
     * Approx
     */

    /**
     * For meaningful approximation, the parent relation must obtain an approximate version with approxWith method.
     */
    @Override
    public ApproxRelation approx() throws VerdictException {
        // no approx
        return ApproxSingleRelation.asis(this);
    }

    @Override
    protected List<ApproxRelation> nBestSamples(Expr elem, int n) throws VerdictException {
        // refresh meta data if needed.
        String schema = getTableName().getSchemaName();
        vc.getMeta().refreshSampleInfoIfNeeded(schema);

        List<ApproxRelation> samples = new ArrayList<ApproxRelation>();

        // Get all the samples
        List<Pair<SampleParam, TableUniqueName>> availableSamples = vc.getMeta().getSampleInfoFor(getTableName());
        // add a relation itself in case there's no available sample.
        availableSamples.add(Pair.of(asSampleParam(), getTableName()));

        for (Pair<SampleParam, TableUniqueName> pair : availableSamples) {
            SampleParam param = pair.getLeft();
            double samplingProb = samplingProb(param, elem);
            if (samplingProb < 0) {
                continue;
            }
            ApproxRelation a = new ApproxSingleRelation(vc, pair.getLeft());
            a.setAlias(getAlias());
            samples.add(a);
        }

        return samples;
    }

    @Override
    protected List<SampleGroup> findSample(Expr elem) {
        // refresh meta data if needed.
        String schema = getTableName().getSchemaName();
        vc.getMeta().refreshSampleInfoIfNeeded(schema);

        // Now the main procedure starts.
        List<SampleGroup> candidates = new ArrayList<SampleGroup>();

        // Get all the samples
        List<Pair<SampleParam, TableUniqueName>> availableSamples = vc.getMeta().getSampleInfoFor(getTableName());
        // add a relation itself in case there's no available sample.
        availableSamples.add(Pair.of(asSampleParam(), getTableName()));

        // If there's no sample; we do not know the size of the original table. In this case, we simply assume the
        // size is 1M.
        double originalTableSize = 1e6;
        SampleSizeInfo si = vc.getMeta().getSampleSizeOf(availableSamples.get(0).getRight());
        if (si != null) {
            originalTableSize = si.originalTableSize;
        }

        for (Pair<SampleParam, TableUniqueName> p : availableSamples) {
            SampleParam param  = p.getLeft();

            //			SampleSizeInfo sizeInfo = vc.getMeta().getSampleSizeOf(p.getRight());
            //			double sampleTableSize = originalTableSize;
            //			if (sizeInfo != null) {		// if not an original table
            //				sampleTableSize = (double) sizeInfo.sampleSize;
            //			}
            //			double samplingProb = samplingProb(p.getLeft(), elem);

            ApproxRelation a = new ApproxSingleRelation(vc, param);
            candidates.add(new SampleGroup(a, Arrays.asList(elem)));

            //			if (samplingProb >= 0) {
            //				
            //			}
        }

        return candidates;
    }

    /**
     * Computes an effective sampling probability for a given sample and an aggregate expression to compute with the sample.
     * A negative return value indicates that the sample must not be used.
     * @param param
     * @param expr
     * @return
     */
    private double samplingProb(SampleParam param, Expr expr) {
        // extract all aggregate functions out of the select list element.
        ExprVisitor<List<FuncExpr>> collectAggFuncs = new ExprVisitor<List<FuncExpr>>() {
            private List<FuncExpr> seen = new ArrayList<FuncExpr>();
            public List<FuncExpr> call(Expr expr) {
                if (expr instanceof FuncExpr) {
                    seen.add((FuncExpr) expr);
                }
                return seen;
            }
        };
        List<FuncExpr> funcs = collectAggFuncs.visit(expr);

        // it is almost always expected that "expr" includes at least one aggregate function, but we place this
        // just in case.
        if (funcs.size() == 0) return param.samplingRatio;

        Set<String> cols = vc.getMeta().getColumns(getTableName());
        List<Double> probs = new ArrayList<Double>();
        for (FuncExpr fexpr : funcs) {
            String fcol = fexpr.getUnaryExprInString();
            if (fexpr.getUnaryExpr() instanceof ColNameExpr) {
                fcol = ((ColNameExpr) fexpr.getUnaryExpr()).getCol();
            }

            if (fexpr.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
                if (cols.contains(fcol)) {
                    if (param.sampleType.equals("universe") && param.columnNames.contains(fcol)) {
                        probs.add(param.samplingRatio);
                    } else if (param.sampleType.equals("stratified") && param.columnNames.contains(fcol)) {
                        probs.add(1.0);
                    } else if (param.sampleType.equals("nosample")) {
                        probs.add(1.0);
                    } else {
                        return -1;		// uniform random samples must not be used for COUNT-DISTINCT
                    }
                } else {
                    if (!param.sampleType.equals("nosample")) {
                        return -1;		// no sampled table should be joined for count-distinct.
                    } else {
                        probs.add(1.0);
                    }
                }
            }
            else if (fexpr.getFuncName().equals(FuncExpr.FuncName.COUNT)
                    || fexpr.getFuncName().equals(FuncExpr.FuncName.SUM)
                    || fexpr.getFuncName().equals(FuncExpr.FuncName.AVG)) {	// COUNT, SUM, AVG
                SampleSizeInfo size = vc.getMeta().getSampleSizeOf(param.sampleTableName());

                if (size == null) {
                    probs.add(1.0);		// the original table
                } else if (param.sampleType.equals("stratified") && param.columnNames.contains(fcol)) {
                    return -1;
                } else {
                    probs.add(size.sampleSize / (double) size.originalTableSize);
                }
            }
            else {    // MIN, MAX
                if (!param.sampleType.equals("nosample")) {
                    return -1;      // no sampled table should be joined for count-distinct.
                } else {
                    probs.add(1.0);
                }
            }
        }

        // returns the harmonic mean of probs
        double hmean = 0;
        for (Double p : probs) {
            hmean += 1.0 / p;
        }
        hmean = probs.size() / hmean;

        return hmean;
    }

    private double costOfSample(SampleParam param, List<Expr> aggExprs) {
        double cost_sum = 0;

        //		return param.samplingRatio * param.

        //		Set<String> cols = new HashSet<String>(vc.getMeta().getColumnNames(getTableName()));
        //		
        //		ExprVisitor<List<FuncExpr>> collectAggFuncs = new ExprVisitor<List<FuncExpr>>() {
        //			private List<FuncExpr> seen = new ArrayList<FuncExpr>();
        //			public List<FuncExpr> call(Expr expr) {
        //				if (expr instanceof FuncExpr) {
        //					seen.add((FuncExpr) expr);
        //				}
        //				return seen;
        //			}
        //		};
        //		
        //		for (Expr aggExpr : aggExprs) {
        //			List<FuncExpr> funcs = collectAggFuncs.visit(aggExpr);
        //			
        //			for (FuncExpr f : funcs) {
        //				String fcol = f.getExprInString();
        //				if (f.getExpr() instanceof ColNameExpr) {
        //					fcol = ((ColNameExpr) f.getExpr()).getCol();
        //				}
        //				if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT) && cols.contains(fcol)) {
        //					if (param.sampleType.equals("universe")
        //							&& param.columnNames.contains(fcol)) {
        //						cost_sum += 50;
        //					} else if (param.sampleType.equals("stratifeid")
        //							&& param.columnNames.contains(fcol)) {
        //						cost_sum += 40;
        //					} else if (param.sampleType.equals("nosample")) {
        //					} else {
        //						cost_sum -= 100;
        //					}
        //				} else if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
        //					if (param.sampleType.equals("nosample")) {
        //					} else {
        //						cost_sum -= 50;
        //					}
        //				} else {
        //					if (param.sampleType.equals("nosample")) {
        //					} else {
        //						cost_sum += 10;
        //					}
        //				}
        //			}
        //		}
        //		
        return cost_sum / aggExprs.size();
    }

    protected ApproxSingleRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
        if (replace.containsKey(getTableName())) {
            ApproxSingleRelation a = ApproxSingleRelation.from(vc, replace.get(getTableName()));
            a.setAlias(getAlias());
            return a;
        } else {
            ApproxSingleRelation a = ApproxSingleRelation.asis(this);
            a.setAlias(getAlias());
            return a;
        }
    }

    /*
     *  Aggregation functions
     */

    //	protected String tableSourceExpr(SingleSourceRelation source) {
    //		if (source.isDerivedTable()) {
    //			return source.toSql();
    //		} else {
    //			return source.tableNameExpr();
    //		}
    //	}

    /*
     * Helpers
     */

    protected SampleParam asSampleParam() {
        return new SampleParam(vc, getTableName(), NOSAMPLE, 1.0, null);
    }

    //	@Override
    //	public List<SelectElem> getSelectList() {
    //		TableUniqueName table = getTableName();
    //		Set<String> columns = vc.getMeta().getColumns(table);
    //		List<SelectElem> elems = new ArrayList<SelectElem>();
    //		for (String c : columns) {
    //			elems.add(new SelectElem(new ColNameExpr(c, table.getTableName())));
    //		}
    //		return elems;
    //	}

    @Override
    public ColNameExpr partitionColumn() {
        Set<String> columns = vc.getMeta().getColumns(getTableName());
        String partitionCol = vc.getConf().subsamplingPartitionColumn();
        if (columns.contains(partitionCol)) {
            return new ColNameExpr(partitionCol, getAlias());
        } else {
            VerdictLogger.error(this, "partition column does not exists in the table: " + getTableName());
            return null;
        }
    }

    @Override
    public List<ColNameExpr> accumulateSamplingProbColumns() {
        List<ColNameExpr> samplingProbCols = new ArrayList<ColNameExpr>();
        Set<String> cols = vc.getMeta().getColumns(tableName);
        String samplingProbColName = samplingProbabilityColumnName();
        for (String c : cols) {
            if (c.equals(samplingProbColName)) {
                samplingProbCols.add(new ColNameExpr(samplingProbColName, tableName.getTableName()));
            }
        }
        return samplingProbCols;
    }

    @Override
    protected String toStringWithIndent(String indent) {
        StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("%s(%s, %s)\n", this.getClass().getSimpleName(), getTableName(), getAlias()));
        return s.toString();
    }
}
