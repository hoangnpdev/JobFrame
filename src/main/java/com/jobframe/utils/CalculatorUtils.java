package com.jobframe.utils;

import java.util.stream.Collectors;

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

    public static Object sum(Object... values) {
        if (TypeUtils.isRealNumber(values[0])) {
            double sum = 0.0;
            for (Object value: values) {
                double rValue = TypeUtils.toDouble(value);
                sum += rValue;
            }
            return sum;
        }
        long sum = 0L;
        for (Object value: values) {
            long lValue = (long) value;
            sum += lValue;
        }
        return sum;
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
