import com.jobframe.core.Column;
import com.jobframe.core.JobFrame;
import com.jobframe.core.JobFrames;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;


@ExtendWith(MockitoExtension.class)
public class LoadingCsvTest {

	private static Logger log = Logger.getLogger(LoadingCsvTest.class);

	private JobFrame firstFrame;

	@BeforeEach
	public void beforeA() throws FileNotFoundException {

	}

	@Test
	public void test_loadingCsv() throws IOException {
		firstFrame = JobFrames.load("src/test/resources/first.csv", Arrays.asList("id", "name", "value"));
		Object out = firstFrame.at(0, "name");
		System.out.println(out);
		assert out.equals("hoang30");
	}

	@Test
	public void test_column() throws FileNotFoundException {
		Column col = new Column(String.class);
		col.append("hoangnp");
		log("size: " + col.size());
		col.append("hoangnp2");
		log("size: " + col.size());
		col.append("hoangnp3");
		log("size: " + col.size());


		Object obj = col.get(0);
		System.out.println(obj);
		assert obj.equals("hoangnp");


		Object obj2 = col.get(1);
		System.out.println(obj2);
		assert obj.equals("hoangnp2");


		Object obj3 = col.get(2);
		System.out.println(obj3);
		assert obj3.equals("hoangnp3");
	}

	public static void log(Object d) {
		System.out.println(d);
	}

}