import com.jobframe.core.Column;
import com.jobframe.core.JobFrame;
import com.jobframe.core.JobFrames;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
		assert out.equals("hoang1");

		Object out2 = firstFrame.at(1, "id");
		System.out.println(out2);
		assert out2.equals(2L);

		Object out3 = firstFrame.at(1, "value");
		System.out.println(out3);
		assert out3.equals(20.0);
	}

	@Test
	public void test_column() throws FileNotFoundException {
		Column col = new Column(String.class);
		col.append("hoangnp");
		col.append("hoangnp2");
		col.append("hoangnp3");


		Object obj = col.get(0);
		System.out.println(obj);
		assert obj.equals("hoangnp");


		Object obj2 = col.get(1);
		System.out.println(obj2);
		assert obj2.equals("hoangnp2");


		Object obj3 = col.get(2);
		System.out.println(obj3);
		assert obj3.equals("hoangnp3");
	}

	@Test
	@Disabled
	public void test_arrLogic() {
		byte[] a = {0x23, 0x00, 0x10, 0x10};
		byte[] b = Arrays.copyOf(a, 5);
		for (byte e: b) {
			log(e);
		}
	}

	public static void log(Object d) {
		System.out.println(d);
	}

}