package com.parallelmachines.reflex.common.enums;

public enum ArchiveType {
	TAR ("tar"),

	ZIP ("zip"),

	TGZ ("tgz"),

	GZIP ("gzip"),

	UNKNOWN ("unknown");

	private final String name;

	ArchiveType(String s) {
		name = s;
	}

	@Override
	public String toString() {
		return name;
	}

	public static ArchiveType fromString(String name) {
		if (name != null) {
			for (ArchiveType at : ArchiveType.values()) {
				if (name.equalsIgnoreCase(at.name)) {
					return at;
				}
			}
		}
		return UNKNOWN;
	}

	public static ArchiveType fromFilename(String fileName) {
		if (fileName.endsWith(".tar")) {
			return ArchiveType.TAR;
		} else if (fileName.endsWith(".tgz") || fileName.endsWith(".tar.gz")) {
			return ArchiveType.TGZ;
		} else if (fileName.endsWith(".zip")) {
			return ArchiveType.ZIP;
		} else {
			return ArchiveType.UNKNOWN;
		}
	}

	// Default archive type is tar.gz and ECO will convert any other archive type to tar.gz internally
	public static boolean needToExtractModel(ArchiveType archiveType) {
		return archiveType == TAR || archiveType == ZIP;
	}
}
