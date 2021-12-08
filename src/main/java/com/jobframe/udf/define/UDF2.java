package com.jobframe.udf.define;

public interface UDF2<T1, T2, R> {
    R apply(T1 arg1, T2 arg2);
}
