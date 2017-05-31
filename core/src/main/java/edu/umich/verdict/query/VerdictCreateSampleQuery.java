package edu.umich.verdict.query;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

import static edu.umich.verdict.util.NameHelpers.*;

import java.sql.ResultSet;

public class VerdictCreateSampleQuery extends VerdictQuery {
	
	private final double sampleRatio = 0.01;

	public VerdictCreateSampleQuery(String q, VerdictContext vc) {
		super(q, vc);
	}
	
	public VerdictCreateSampleQuery(VerdictQuery parent) {
		super(parent.queryString, parent.vc);
	}
	
	@Override
	public ResultSet compute() throws VerdictException {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(queryString));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		
		VerdictSQLBaseVisitor<Pair<String, Optional<Double>>> visitor = new VerdictSQLBaseVisitor<Pair<String, Optional<Double>>>() {			
			public Pair<String, Optional<Double>> visitCreate_sample_statement(VerdictSQLParser.Create_sample_statementContext ctx) {
				Optional<Double> ratio = Optional.absent();
				if (ctx.size != null) {
					ratio = Optional.of(0.01 * Double.valueOf(ctx.size.getText()));
				}
				return Pair.of(ctx.table_name().getText(), ratio);
			}
		};
		
		Pair<String, Optional<Double>> TableNameAndRatio = visitor.visit(p.create_sample_statement());; 
		String originalTableName = TableNameAndRatio.getLeft();
		Optional<Double> ratio = TableNameAndRatio.getRight();
		
		VerdictLogger.info(this, String.format("Create a %.4f percentage sample of %s.", ratio.or(sampleRatio)*100, originalTableName));
		Pair<Long, Long> sampleAndOriginalSizes = vc.getDbms().createSampleTableOf(originalTableName, ratio.or(sampleRatio));
		vc.getMeta().insertSampleInfo(schemaOfTableName(vc.getCurrentSchema(), originalTableName).get(), tableNameOfTableName(originalTableName),
				sampleAndOriginalSizes.getLeft(), sampleAndOriginalSizes.getRight());
		return null;
	}

}
