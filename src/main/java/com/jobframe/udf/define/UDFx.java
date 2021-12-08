package com.jobframe.udf.define;

public interface UDFx<T1, R> {
    R apply(T1 args);
}
