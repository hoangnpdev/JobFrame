
import com.jobframe.core.Column;
import com.jobframe.core.JobFrame;
import com.jobframe.core.JobFrames;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;

import static com.jobframe.core.ExpressionBuilder.*;


@ExtendWith(MockitoExtension.class)
public class JobFrameBasicTest {

	private static Logger log = Logger.getLogger(JobFrameBasicTest.class);

	private JobFrame firstFrame;

	private JobFrame secondFrame;

	private JobFrame duplicatedFrame;

	private JobFrame thirdFrame;

	private JobFrame groupFrame;

	@BeforeEach
	public void beforeA() {
		firstFrame = JobFrames.load("src/test/resources/first.csv", Arrays.asList("id", "name", "value"));

		secondFrame = JobFrames.load("src/test/resources/second.csv", Arrays.asList("id", "name", "value"));

		duplicatedFrame = JobFrames.load("src/test/resources/duplicate.csv", Arrays.asList("id", "name2", "value2"));

		thirdFrame = JobFrames.load("src/test/resources/third.csv", Arrays.asList("id", "name", "value1", "value2"));

		groupFrame = JobFrames.load("src/test/resources/group.csv", Arrays.asList("id", "name", "value1", "value2"));
	}

	@Test
	public void test_getColumnTypeAndSize() {
		Column col = firstFrame.getColumn("name");
		assert col.size() == 4;
		assert col.type() == String.class;
	}

	@Test
	public void test_getValueAt() {
		Object data = firstFrame.at(0, "name");
		assert data.equals("hoang1");
	}

	@Test
	public void test_eqAndGet() {
		JobFrame eqFrame = firstFrame.eqAndGet("name", "hoang3");
		assert eqFrame.at(0, "value").equals(30.0);
	}

	@Test
	public void test_joinInnerSize() {
		JobFrame joinFrame = firstFrame.join(
				secondFrame,
				"id=id",
				"inner"
		);
		JobFrame nFrame = joinFrame.eqAndGet("name", "hoang4");
		System.out.println(nFrame.at(0, "value"));
		assert nFrame.at(0, "value").equals(40.0);
	}

	@Test
	public void test_sequenceInnerJoin() {
		assert firstFrame.join(
				secondFrame,
				"id=id",
				"inner"
		).join(
				thirdFrame,
				"id=id",
				"inner"
		).size() == 2;
	}

	@Test
	public void test_leftJoin() {
		// 1 2 3 3 4
		JobFrame joinFrame = firstFrame.join(
				duplicatedFrame,
				"id=id",
				"left"
		);
		assert joinFrame
				.where(
						col("id").equalTo(lit(3L))
				).size() == 2;
		assert joinFrame
				.where(
						col("id").equalTo(lit(1L))
				).at(0, "value2") == null;
		assert joinFrame
				.where(
						col("id").equalTo(lit(4L))
				).at(0, "value2").equals(400.0);
	}



	@Test
	public void test_fullJoin() {
		// 1 2 3 3 4
		JobFrame joinFrame = firstFrame.join(
				duplicatedFrame,
				"id=id",
				"full"
		);
		assert joinFrame
				.where(
						col("id").equalTo(lit(3L))
				).size() == 2;
		assert joinFrame
				.where(
						col("id").equalTo(lit(1L))
				).at(0, "value2") == null;
		assert joinFrame
				.where(
						col("id").equalTo(lit(6L))
				).at(0, "value") == null;
		assert joinFrame
				.where(
						col("name2").equalTo(lit("hoang60"))
				).at(0, "value2").equals(600.0);
	}

	@Test
	public void test_where() {
		JobFrame whereFrame = firstFrame.where(
				col("name").equalTo(lit("hoang2"))
				.or(col("value").equalTo(lit(40.0)))
		);
		assert whereFrame.size() == 2;
	}

	@Test
	public void test_select() {
		JobFrame selectFrame = firstFrame.select("name", "value");
		assert selectFrame.columns().size() == 2;
	}

	@Test
	public void test_withColumnExpression() {
		JobFrame sumFrame = thirdFrame.withColumn(
				"sum",
				col("value1").add(col("value2"))
		);
		assert sumFrame.at(0, "sum").equals(310.0);
		assert sumFrame.at(3, "sum").equals(605.0);
	}

	@Test
	public void test_groupByOneColumn() {
		JobFrame groupedFrame = groupFrame.groupBy("name")
				.sum("value1");
		assert groupedFrame.where(
					col("name").equalTo(lit("hoang60"))
				).at(0, "value1").equals(1200.0);
	}

	@Test
	public void test_groupByMultiColumn() {
		JobFrame groupedFrame = groupFrame.groupBy("id", "name")
				.sum("value1");
		assert groupedFrame.where(
				col("name").equalTo(lit("hoang40"))
		).at(0, "value1").equals(900.0);
	}

	@Test
	public void test_UDF1() {
		JobFrame udfFrame = firstFrame.withColumn("udf_column", (Double v) -> v * v, "value");
		assert udfFrame.at(0, "udf_column").equals(100.0);
	}

	@Test
	public void test_UDF2() {
		JobFrame udfFrame = firstFrame.withColumn("udf_column", (Long c1, Double c2) -> c1 * c2, "id", "value");
		assert udfFrame.at(2, "udf_column").equals(90.0);
	}

	@Test
	public void test_UDF3() {
		JobFrame udfFrame = firstFrame.withColumn(
				"udf_column",
				(Long c1, String c2, Double c3) -> c1 * c2.length() * c3,
				"id", "name", "value"
		);
		assert udfFrame.at(3, "udf_column").equals(960.0);
	}

	@Test
	public void test_SortOneColumnAsc() {
		JobFrame sortedFrame = groupFrame.sort("value1");
		assert sortedFrame.at(0, "value1").equals(200.0);
		assert sortedFrame.at(5, "value1").equals(600.0);
	}
}
