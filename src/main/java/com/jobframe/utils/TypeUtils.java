package com.jobframe.utils;

public class TypeUtils {

    public static boolean isRealNumber(Object value) {
        if (value instanceof Double || value instanceof Float) {
            return true;
        }
        return false;
    }

    public static boolean isNaturalNumber(Object value) {
        return !isRealNumber(value);
    }

    public static double toDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        throw new RuntimeException("value is not instance of Number type");
    }


}
