import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DeckGlobalStatsJob extends Configured implements Tool {


    public static class DeckGlobalStatsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private HashMap<String, Integer> deckOccurences = new HashMap<>();
        private int nbDeck = 0;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            URI[] files = context.getCacheFiles(); 

            if (files != null && files.length > 0) {
                for (URI p : files) {
                    File f = new File("./" + new File(p.getPath()).getName());
                    BufferedReader rdr = new BufferedReader(
                            new InputStreamReader(new FileInputStream(f)));
                    String line = null;
                    while ((line = rdr.readLine()) != null) {
                        String[] tokens = line.split(";");
                        if (tokens.length >= 3) {
                            String deck = tokens[0];
                            Integer deckOccurence = Integer.parseInt(tokens[1]);
                            nbDeck += deckOccurence;
                            deckOccurences.put(deck, deckOccurence);
                        }
                    }
                    rdr.close();
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Read line formatted as : "source;target;totalGames;totalWins"
            String line = value.toString();
            String[] data = line.split(";");
            if (data.length != 4) {
                return; // Ignore malformed lines
            }

            String deckSource = data[0];
            String deckTarget = data[1];
            int totalGames = Integer.parseInt(data[2]);
            int totalWins = Integer.parseInt(data[3]);

            // Recuperate occurences of the 2 archetypes
            int occurenceDeckSource = deckOccurences.getOrDefault(deckSource, 0);
            int occurenceDeckTarget = deckOccurences.getOrDefault(deckTarget, 0);

            // Calculate
            double stat = (double) occurenceDeckSource * occurenceDeckTarget / (double) (nbDeck);
            // Get stat with only 2 decimals
            stat = Math.round(stat * 100.0) / 100.0;

            context.write(new Text(deckSource + ';' + deckTarget + ";" + totalGames + ";" + totalWins + ";" + occurenceDeckSource
                    + ";" + occurenceDeckTarget + ";" + stat), NullWritable.get());
        }

    }

    // Reducer : Somme les occurrences, filtre et formate
    public static class DeckGlobalStatsReducer extends Reducer<Text, NullWritable, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {

            context.write(NullWritable.get(), key);

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: deckglobalstats <input path> <output path> <archetype file>");
            return -1;
        }
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "Clash Royale Deck Global Stats");
        job.addCacheFile(new URI(args[2]));

        job.setJarByClass(DeckGlobalStatsJob.class);

        job.setMapperClass(DeckGlobalStatsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(DeckGlobalStatsReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DeckGlobalStatsJob(), args);
        System.exit(res);
    }
}