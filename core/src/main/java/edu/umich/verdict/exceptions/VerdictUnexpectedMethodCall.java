package edu.umich.verdict.exceptions;

public class VerdictUnexpectedMethodCall extends VerdictException {
	
	public VerdictUnexpectedMethodCall() {
		this("");
	}

	public VerdictUnexpectedMethodCall(String message) {
		super(message);
	}

}
