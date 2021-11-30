import static com.jobframe.core.ExpressionBuilder.*;

import com.jobframe.core.Expression;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ExpressionTest {

    @Test
    public void test_expressionLit() {
        Expression expression = lit(20L);
        assert expression.calculate(null).equals(20L);
        Expression expression1 = lit("abc");
        assert expression1.calculate(null).equals("abc");
        assert !expression1.calculate(null).equals(20L);
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
        assert  expression.calculate(null).equals(200L);
    }

    @Test
    public void test_divide() {
        Expression expression = lit(10L).divide(lit(20L));
        System.out.println(expression.calculate(null));
        assert  expression.calculate(null).equals(0.5);
    }
}
