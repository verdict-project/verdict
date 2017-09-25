package edu.umich.verdict;

import java.sql.ResultSet;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import edu.umich.verdict.dbms.DbmsSpark2;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.query.Query;
import edu.umich.verdict.util.VerdictLogger;

public class VerdictSpark2HiveContext extends VerdictContext {

    private Dataset<Row> df;

    public VerdictSpark2HiveContext(SparkContext sc) throws VerdictException {
        this(sc, new VerdictConf());
    }

    public VerdictSpark2HiveContext(SparkContext sc, VerdictConf conf) throws VerdictException {
        super(conf);
        conf.setDbms("spark2");
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        setDbms(new DbmsSpark2(this, sparkSession));
        setMeta(new VerdictMeta(this));
    }

    @Override
    public void execute(String sql) throws VerdictException {
        VerdictLogger.debug(this, "An input query:");
        VerdictLogger.debugPretty(this, sql, "  ");
        Query vq = Query.getInstance(this, sql);
        df = vq.computeDataset();
    }

    @Override
    public ResultSet getResultSet() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Dataset<Row> getDataset() {
        return df;
    }

    @Override
    public DataFrame getDataFrame() {
        return null;
    }
}
