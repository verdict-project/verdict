package edu.umich.verdict.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class StackTraceReader {

	public static String stackTrace2String(Exception e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString();
	}
	
}
