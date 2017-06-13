package edu.umich.verdict.relation.functions;


public class AggFunction {
	
	private String expression;
	
	private String functionPattern;
	
	public AggFunction(String expression, String functionPattern) {
		this.expression = expression;
		this.functionPattern = functionPattern;
	}
	
	public static AggFunction count() {
		return new AggFunction("*", "COUNT(%s)");
	}
	
	public static AggFunction countDistinct(String expression) {
		return new AggFunction(expression, "COUNT(DISTINCT %s)");
	}

	public String call() {
		return String.format(functionPattern, expression);
	}

}
