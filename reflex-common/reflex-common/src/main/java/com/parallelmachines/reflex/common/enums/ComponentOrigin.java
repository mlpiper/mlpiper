package com.parallelmachines.reflex.common.enums;

/**
 * This enum will provide the class of the component
 */

public enum ComponentOrigin {

    BUILTIN("builtin"),                 // Builtin components are shipped with the product

    UPLOADED("uploaded"),               // Uploaded components are uploaded and managed by mlops

    SOURCE_CONTROL("source_control"),   // Source control components are registered at the mlops, but are managed by
                                        // the source control repo e.g. Git.

    UNKNOWN("unknown");                 // For errors.

    private final String name;

    ComponentOrigin(String name) {
        this.name = name;
    }

    public static ComponentOrigin fromString(String name) {
        if (name != null) {
            for (ComponentOrigin compClass : ComponentOrigin.values()) {
                if (name.equalsIgnoreCase(compClass.name)) {
                    return compClass;
                }
            }
        }
        return UNKNOWN;
    }

    @Override
    public String toString() {
        return this.name;
    }
}


