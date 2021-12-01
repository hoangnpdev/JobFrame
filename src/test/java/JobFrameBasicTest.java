
import com.jobframe.core.Column;
import com.jobframe.core.JobFrame;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;

import static com.jobframe.core.ExpressionBuilder.*;


@ExtendWith(MockitoExtension.class)
public class JobFrameBasicTest {

	private static Logger log = Logger.getLogger(JobFrameBasicTest.class);

	private JobFrame jobFrame;

	private JobFrame otherFrame;

	private JobFrame fourFrame;

	@BeforeEach
	public void beforeA() {
		List<List<Object>> datas = Arrays.asList(
				Arrays.asList(1L, "hoang1", 10.0),
				Arrays.asList(2L, "hoang2", 20.0),
				Arrays.asList(3L, "hoang3", 30.0),
				Arrays.asList(4L, "hoang4", 40.0)
		);
		jobFrame = new JobFrame(datas, Arrays.asList("id", "name", "value"));


		List<List<Object>> datas2 = Arrays.asList(
				Arrays.asList(3L, "hoang30", 300.0),
				Arrays.asList(4L, "hoang40", 400.0),
				Arrays.asList(5L, "hoang50", 500.0),
				Arrays.asList(6L, "hoang60", 600.0)
		);
		otherFrame = new JobFrame(datas2, Arrays.asList("id", "name", "value"));

		List<List<Object>> datas3 = Arrays.asList(
				Arrays.asList(3L, "hoang30", 300.0, 10),
				Arrays.asList(4L, "hoang40", 400.0, 5),
				Arrays.asList(5L, "hoang50", 500.0, 10),
				Arrays.asList(6L, "hoang60", 600.0, 5)
		);
		fourFrame = new JobFrame(datas3, Arrays.asList("id", "name", "value1", "value2"));
	}

	@Test
	public void test_getColumnTypeAndSize() {
		Column col = jobFrame.getColumn("name");
		assert col.size() == 4;
		assert col.type() == String.class;
	}

	@Test
	public void test_getValueAt() {
		Object data = jobFrame.at(0, "name");
		assert data.equals("hoang1");
	}

	@Test
	public void test_eqAndGet() {
		JobFrame eqFrame = jobFrame.eqAndGet("name", "hoang3");
		assert eqFrame.at(0, "value").equals(30.0);
	}

	@Test
	public void test_joinInnerSize() {
		JobFrame joinFrame = jobFrame.join(
				otherFrame,
				"id=id",
				"inner"
		);
		JobFrame nFrame = joinFrame.eqAndGet("name", "hoang4");
		System.out.println(nFrame.at(0, "value"));
		assert nFrame.at(0, "value").equals(40.0);
	}

	@Test
	public void test_where() {
		JobFrame whereFrame = jobFrame.where(
				col("name").equalTo(lit("hoang2"))
				.or(col("value").equalTo(lit(40.0)))
		);
		assert whereFrame.size() == 2;
	}

	@Test
	public void test_select() {
		JobFrame selectFrame = jobFrame.select("name", "value");
		assert selectFrame.columns().size() == 2;
	}

	@Test
	public void test_withColumnExpression() {
		JobFrame sumFrame = fourFrame.withColumn(
				"sum",
				col("value1").add(col("value2"))
		);
		assert sumFrame.at(0, "sum").equals(310.0);
		assert sumFrame.at(3, "sum").equals(605.0);
	}

	@Test
	public void test_withColumnUdf() {
		JobFrame udfFrame = fourFrame.withColumn(
				"result",
				(double a, long b) -> {
					return a / (b * b);
				}, "value1", "value2"
		);
	}

}
