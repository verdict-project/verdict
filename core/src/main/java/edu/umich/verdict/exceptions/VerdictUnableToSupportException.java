package edu.umich.verdict.exceptions;

public class VerdictUnableToSupportException extends VerdictException {

	public VerdictUnableToSupportException(String message) {
		super(message);
	}

	public VerdictUnableToSupportException(Exception e) {
		super(e);
	}

}
