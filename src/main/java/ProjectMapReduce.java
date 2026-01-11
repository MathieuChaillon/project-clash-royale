
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
			pgd.addClass("globalstats", GlobalStatsJob.class, "compute global statistics");

			pgd.addClass("deckcode", DeckToDeckCode.class, "compute deck code");
			pgd.addClass("deckstats", DeckStatsJob.class, "compute deck statistics");
			pgd.addClass("deckglobalstats", DeckGlobalStatsJob.class, "compute deck global statistics");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}

}
