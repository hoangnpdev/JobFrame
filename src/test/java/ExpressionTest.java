import static com.jobframe.core.ExpressionBuilder.*;

import com.jobframe.core.Expression;
import com.jobframe.core.JobFrame;
import com.jobframe.core.JobFrames;
import com.jobframe.core.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@ExtendWith(MockitoExtension.class)
public class ExpressionTest {

    private JobFrame jobFrame;

    @BeforeEach
    public void beforeA() throws IOException {
        jobFrame = JobFrames.load("src/test/resources/first.csv", Arrays.asList("id", "name", "value"));
    }

    @Test
    public void test_expressionLit() {
        Expression expression = lit(20L);
        assert expression.calculate(null).equals(20L);
        Expression expression1 = lit("abc");
        assert expression1.calculate(null).equals("abc");
        assert !expression1.calculate(null).equals(20L);
    }

    @Test
    public void test_expressionCol() {
        Expression expression = col("name");
        Row row = jobFrame.getRow(0);
        assert expression.calculate(row).equals("hoang1");
    }

    @Test
    public void test_addition() {
        Expression expression = lit(10L).add(lit(20L));
        assert  expression.calculate(null).equals(30L);
    }

    @Test
    public void test_subtraction() {
        Expression expression = lit(10L).subtract(lit(20.0));
        assert  expression.calculate(null).equals(-10.0);
    }

    @Test
    public void test_multiply() {
        Expression expression = lit(10L).multiply(lit(20L));
        assert expression.calculate(null).equals(200L);
    }

    @Test
    public void test_divide() {
        Expression expression = lit(10L).divide(lit(20L));
        System.out.println(expression.calculate(null));
        assert expression.calculate(null).equals(0.5);
    }

    @Test
    public void test_and() {
        Expression expression = lit(true).and(lit(false));
        assert expression.calculate(null).equals(false);
    }

    @Test
    public void test_or() {
        Expression expression = lit(true).or(lit(false));
        assert expression.calculate(null).equals(true);
    }

    @Test
    public void test_not() {
        Expression expression = not(lit(false));
        assert expression.calculate(null).equals(true);
    }
}
