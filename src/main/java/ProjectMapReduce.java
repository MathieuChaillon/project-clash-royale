
import java.io.IOException;

import org.apache.hadoop.util.ProgramDriver;

public class ProjectMapReduce {

	public static void main(String[] args) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("filter", FilterGame.class, "filter games");
			pgd.addClass("archetypecode", DeckToArchetypeCode.class, "compute deck popularity");
			pgd.addClass("archetypestats", ArchetypeStatsJob.class, "compute archetype statistics");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}

}
