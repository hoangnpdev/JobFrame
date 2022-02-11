import com.jobframe.core.Column;
import com.jobframe.core.JobFrame;
import com.jobframe.core.JobFrames;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.FileNotFoundException;
import java.util.Arrays;


@ExtendWith(MockitoExtension.class)
public class LoadingCsvTest {

	private static Logger log = Logger.getLogger(LoadingCsvTest.class);

	private JobFrame firstFrame;

	@BeforeEach
	public void beforeA() throws FileNotFoundException {

	}

	@Test
	public void test_loadingCsv() throws FileNotFoundException {
		firstFrame = JobFrames.load("src/test/resources/first.csv", Arrays.asList("id", "name", "value"));
		Object out = firstFrame.at(0, "name");
		out.equals("hoang30");
	}

	@Test
	public void test_column() throws FileNotFoundException {
		Column col = new Column(String.class);
		col.append("hoangnp");
		Object obj = col.get(0);
		assert obj.equals("hoangnp");
	}

}