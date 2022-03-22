package com.jobframe.util;

import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Int2DStream {

    public static <T> Stream<T> shape(Integer x, Integer y, Function2D<Integer, Integer, T> mapper) {
        return IntStream.range(0, x * y)
                .boxed()
                .map(idx -> {
                    int ix = idx / y;
                    int iy = idx % y;
                    return mapper.apply(ix, iy);
                });
    }
}
