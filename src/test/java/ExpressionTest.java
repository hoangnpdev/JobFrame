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
}
