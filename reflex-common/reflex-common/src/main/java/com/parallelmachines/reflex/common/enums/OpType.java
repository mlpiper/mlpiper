package com.parallelmachines.reflex.common.enums;


public enum OpType {

	CATEGORICAL("categorical"),

	CONTINUOUS("continuous");

	private final String value;

	OpType(String v) {
		value = v;
	}

	public String value() {
		return value;
	}

	public static OpType fromValue(String v) {
		for (OpType c: OpType.values()) {
			if (c.value.equals(v)) {
				return c;
			}
		}
		throw new IllegalArgumentException(v);
	}

}