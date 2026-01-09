
import java.io.IOException;

import org.apache.hadoop.util.ProgramDriver;

public class ProjectMapReduce {

	public static void main(String[] args) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("filter", FilterGame.class, "filter games");
			// pgd.addClass("resume", bigdata.worldpop.ResumeCities.class, "Agregate cities according to their population");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}

}
