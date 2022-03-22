package test.jobframe.util;

import com.jobframe.util.Int2DStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.stream.Collectors;
import java.util.stream.Stream;

@ExtendWith(MockitoExtension.class)
public class Int2DStreamTest {

    @Test
    public void test_shape() {
        assert Int2DStream.shape(2, 2, (x, y) -> x + String.valueOf(y))
                .collect(Collectors.joining(","))
                .equals("00,01,10,11");
    }
}
