
import com.jobframe.core.JobFrame;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;


@ExtendWith(MockitoExtension.class)
public class JobFrameTest {

	private static Logger log = Logger.getLogger(JobFrameTest.class);

	private JobFrame jobFrame;

	@BeforeAll
	public void beforeA() {
		List<List<Object>> datas = Arrays.asList(
				Arrays.asList(1L, "hoang1", 10.0),
				Arrays.asList(2L, "hoang2", 20.0),
				Arrays.asList(3L, "hoang3", 30.0),
				Arrays.asList(4L, "hoang4", 40.0)
		);
		jobFrame = new JobFrame(datas, Arrays.asList("id", "name", "value"));
	}

	public void test_getColumnTypeAndSize() {


	}

	public void 
}
