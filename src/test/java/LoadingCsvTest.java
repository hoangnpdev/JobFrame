import com.jobframe.core.JobFrame;
import com.jobframe.core.JobFrames;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;


@ExtendWith(MockitoExtension.class)
public class LoadingCsvTest {

	private static Logger log = Logger.getLogger(LoadingCsvTest.class);

	private JobFrame firstFrame;

	@BeforeEach
	public void beforeA() {
		firstFrame = JobFrames.load("src/test/resources/first.csv", Arrays.asList("id", "name", "value"));
	}

	@Test
	public void test_loadingCsv() {
		firstFrame.at(0, "name").equals("hoang30");
	}
}