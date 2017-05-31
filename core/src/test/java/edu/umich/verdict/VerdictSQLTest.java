package edu.umich.verdict;

import java.util.List;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.VerdictSQLParser.Binary_operator_expressionContext;
import edu.umich.verdict.VerdictSQLParser.Column_ref_expressionContext;
import edu.umich.verdict.VerdictSQLParser.ExpressionContext;
import edu.umich.verdict.VerdictSQLParser.Primitive_expressionContext;


public class VerdictSQLTest {

	public static void main(String[] args) {
		case5();
	}
	
	
	private static void case1() {
		final String sql = "select name, count(*) from customers where name like '%mike%' group by name";
		
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(sql));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
	    
	    System.out.println(new java.util.Date());
	    
	    ParseTreeWalker walker = new ParseTreeWalker();
	    
	    walker.walk(new VerdictSQLBaseListener() {
	    	
	    	public void enterSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx) {
	    		System.out.println("select elem: " + ctx.getText());
	    	}
	    	
	    	public void enterGroup_by_item(VerdictSQLParser.Group_by_itemContext ctx) {
	    		System.out.println("group by: " + ctx.getText());
	    	}
	    	
	    	public void enterSearch_condition(VerdictSQLParser.Search_conditionContext ctx) {
	    		System.out.println("selection predicates: " + getOriginalText(ctx, sql));
	    	}
	    	
	    }, p.select_statement());
	}
	
	// This test tells us that the parser ignores the second query.
	private static void case2() {
		final String sql = "select name, count(*) / sum(A) from customers where name like '%mike%' group by name;"
				+ "select name, count(*) from customers where name like '%cafarella%' group by name;";
		
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(sql));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
	    
	    System.out.println(new java.util.Date());
	    
	    ParseTreeWalker walker = new ParseTreeWalker();
	    
	    walker.walk(new VerdictSQLBaseListener() {
	    	
	    	public void enterSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx) {
	    		System.out.println("select elem: " + ctx.getText());
	    		System.out.println("select elem expression: " + ctx.expression().getText());
	    	}
	    	
	    	public void enterGroup_by_item(VerdictSQLParser.Group_by_itemContext ctx) {
	    		System.out.println("group by: " + ctx.getText());
	    	}
	    	
	    	public void enterSearch_condition(VerdictSQLParser.Search_conditionContext ctx) {
	    		System.out.println("selection predicates: " + getOriginalText(ctx, sql));
	    	}
	    	
	    }, p.select_statement());
	}
	
	private static void case3() {
		final String sql = "select name, count(*) / sum(quantity) + 3"
				+ "from customers where name like '%mike%' group by name;";
		
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(sql));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
	    
	    System.out.println(new java.util.Date());
	    
	    ParseTreeWalker walker = new ParseTreeWalker();
	    
	    walker.walk(new VerdictSQLBaseListener() {
	    	
	    	public void enterSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx) {
	    		System.out.println("select elem: " + ctx.getText());
	    	}
	    	
	    	public void enterGroup_by_item(VerdictSQLParser.Group_by_itemContext ctx) {
	    		System.out.println("group by: " + ctx.getText());
	    	}
	    	
	    	public void enterSearch_condition(VerdictSQLParser.Search_conditionContext ctx) {
	    		System.out.println("selection predicates: " + getOriginalText(ctx, sql));
	    	}
	    	
	    	public void enterAggregate_windowed_function(VerdictSQLParser.Aggregate_windowed_functionContext ctx) {
	    		System.out.println("aggregate function: " + ctx.getText());
	    	}
	    	
	    	public void enterBinary_operator_expression(VerdictSQLParser.Binary_operator_expressionContext ctx) {
	    		System.out.println("binary expression: " + ctx.expression(0).getText() + ", " + ctx.op.getText() + ", " + ctx.expression(1).getText());
	    	}
	    	
	    }, p.select_statement());
	}
	
	private static void case4() {
		final String sql = "select name, count(*) / sum(quantity) + 3, avg(A1) + avg(A2) / avg(A3)"
				+ "from customers where name like '%mike%' group by name;";
		
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(sql));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
	    
	    System.out.println(new java.util.Date());
	    
	    ParseTreeWalker walker = new ParseTreeWalker();
	    
	    walker.walk(new VerdictSQLBaseListener() {
	    	
	    	private int select_expression_id = 0;
	    	
	    	public void enterSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx) {
	    		select_expression_id += 1;
//	    		System.out.format("select elem [%d]: %s\n", select_expression_id, ctx.getText());
	    		ExpressionContext expr_ctx = ctx.expression();
	    		
	    		if (expr_ctx instanceof Primitive_expressionContext) {
	    			System.out.format("primitive expression [%d]: %s\n", select_expression_id, expr_ctx.getText());
	    		}
	    		else if (expr_ctx instanceof Column_ref_expressionContext) {
	    			System.out.format("column expression [%d]: %s\n", select_expression_id, expr_ctx.getText());
	    		}
	    		else if (expr_ctx instanceof Binary_operator_expressionContext) {
	    			System.out.format("binary expression [%d]: %s\n", select_expression_id, expr_ctx.getText());
	    		}
	    	}
	    	
	    	public void enterBinary_operator_expression(VerdictSQLParser.Binary_operator_expressionContext ctx) {
	    		System.out.format("binary expression [%d]. left: %s, op: %s, right: %s\n",
	    				select_expression_id, ctx.expression(0).getText(), ctx.op.getText(), ctx.expression(1).getText());
	    	}
	    },  p.select_statement());
	}
	
	private static void case5() {
		final String sql = "select name, count(*) / sum(quantity) + 3, avg(A1) + avg(A2) / avg(A3)"
				+ "from customers where name like '%mike%' group by name;";
		
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(sql));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		
		VerdictSQLBaseVisitor<Object> visitor = new VerdictSQLBaseVisitor<Object>() {
			
			public Object visitSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx) {
//				System.out.format("select list elem: %s\n", ctx.getText());
				
				if (ctx.expression() instanceof VerdictSQLParser.Column_ref_expressionContext) {
					this.visit(ctx.expression());
				}
				
				if (ctx.expression() instanceof VerdictSQLParser.Binary_operator_expressionContext) {
					this.visit(ctx.expression());
				}
				
				return null;
			}
			
			public Object visitPrimitive_expression(VerdictSQLParser.Primitive_expressionContext ctx) {
				System.out.format("primitive: %s\n", ctx.getText());
				return null;
			}
			
			public Object visitColumn_ref_expression(VerdictSQLParser.Column_ref_expressionContext ctx) {
				
				System.out.format("column ref: %s\n", ctx.getText());
				
				return null;
			}
			
			public Object visitFunction_call(VerdictSQLParser.Function_callContext ctx) {
				
				System.out.format("aggregate function: %s\n", ctx.getText());
				
				return null;
			}
			
			public Object visitBinary_operator_expression(VerdictSQLParser.Binary_operator_expressionContext ctx) {
				
				System.out.format("left: %s, op: %s, right: %s\n", ctx.expression(0).getText(), ctx.op.getText(), ctx.expression(1).getText());
				
				this.visit(ctx.expression(0));
				this.visit(ctx.expression(1));
				
				return null;
			}
			
		};
		
		visitor.visit(p.select_statement().query_expression().query_specification().select_list());
		
//		System.out.println("Second run");
//		
//		visitor.visit(p.select_statement().query_expression().query_specification().select_list());
		
	}
	
	
	private static String getOriginalText(ParserRuleContext ctx, String input) {
		int a = ctx.start.getStartIndex();
	    int b = ctx.stop.getStopIndex();
	    Interval interval = new Interval(a,b);
	    return CharStreams.fromString(input).getText(interval);
	}
	
}
