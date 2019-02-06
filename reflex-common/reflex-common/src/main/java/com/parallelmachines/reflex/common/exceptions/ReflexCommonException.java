package com.parallelmachines.reflex.common.exceptions;

public class ReflexCommonException extends Exception {
	private String message;

	public ReflexCommonException(String message) {
		this.message = message;
	}

	public String getMessage() {
		return message;
	}
}
