package com.jobframe.udf.define;

public interface UDFx<T, R> {
    R apply(T... args);
}
