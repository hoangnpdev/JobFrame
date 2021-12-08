package com.jobframe.udf.define;

public interface UDF3<T1, T2, T3, R> {
    R apply(T1 arg1, T2 arg2, T3 arg3);
}
