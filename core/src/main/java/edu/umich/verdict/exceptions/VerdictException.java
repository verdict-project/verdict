package edu.umich.verdict.exceptions;

import edu.umich.verdict.util.StackTraceReader;

public class VerdictException extends java.lang.Exception {

	private static final long serialVersionUID = 1L;
	
	public VerdictException(String message) {
		super(message);
	}
	
	public VerdictException(Exception e) {
		this(StackTraceReader.stackTrace2String(e));
	}
}
