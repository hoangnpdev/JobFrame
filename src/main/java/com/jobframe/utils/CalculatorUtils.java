package com.jobframe.utils;

public class CalculatorUtils {

    public static Object add(Object value1, Object value2) {
        if (TypeUtils.isRealNumber(value1) || TypeUtils.isRealNumber(value2)) {
            return TypeUtils.toDouble(value1) + TypeUtils.toDouble(value2);
        }
        return (long) value1 + (long) value2;
    }

    public static Object subtract(Object value1, Object value2) {
        if (TypeUtils.isRealNumber(value1) || TypeUtils.isRealNumber(value2)) {
            return TypeUtils.toDouble(value1) - TypeUtils.toDouble(value2);
        }
        return (long) value1 - (long) value2;
    }

    public static Object multiply(Object value1, Object value2) {
        if (TypeUtils.isRealNumber(value1) || TypeUtils.isRealNumber(value2)) {
            return TypeUtils.toDouble(value1) * TypeUtils.toDouble(value2);
        }
        return (long) value1 * (long) value2;
    }

    public static Object divide(Object value1, Object value2) {
        return TypeUtils.toDouble(value1) / TypeUtils.toDouble(value2);
    }

    public static Object and(Object value1, Object value2) {
        return (boolean) value1 && (boolean) value2;
    }

    public static Object or(Object value1, Object value2) {
        return (boolean) value1 || (boolean) value2;
    }

    public static Object not(Object value1) {
        return ! (boolean) value1;
    }
}
