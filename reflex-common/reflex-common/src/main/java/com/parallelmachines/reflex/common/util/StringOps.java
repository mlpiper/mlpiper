package com.parallelmachines.reflex.common.util;


public class StringOps {
    public static String maskPasswords(String str) {
        final String keyword = "password";
        final String replacement = "*****";

        if (str == null || str.isEmpty()) {
            return str;
        } else {
            return str.replaceAll(String.format("(?i)(\"[\\w-]*%s\"\\s*:\\s*).*?(\\s*[,}])", keyword),
                    String.format("$1\"%s\"$2", replacement));
        }
    }
}
