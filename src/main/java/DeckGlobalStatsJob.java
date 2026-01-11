import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.B;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.I;

public class DeckGlobalStatsJob extends Configured implements Tool {

    public enum GameCounters {
        NB_DECK
    }

    public static class DeckGlobalStatsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private HashMap<String, Integer> deckOccurences = new HashMap<>();
        private int nbDeck = 0;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            URI[] files = context.getCacheFiles(); // Récupère les URIs HDFS

            if (files != null && files.length > 0) {
                for (URI p : files) {
                    File f = new File("./" + new File(p.getPath()).getName());
                    BufferedReader rdr = new BufferedReader(
                            new InputStreamReader(new FileInputStream(f)));
                    String line = null;
                    // For each record in the user file
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
            context.getCounter(GameCounters.NB_DECK).increment(nbDeck);

        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // On s'attend à une entrée formatée : "deckA-deckB"
            String line = value.toString();
            String[] data = line.split(";");
            if (data.length != 4) {
                return; // Ignore malformed lines
            }

            String archetype1 = data[0];
            String archetype2 = data[1];
            int totalGames = Integer.parseInt(data[2]);
            int totalWins = Integer.parseInt(data[3]);

            // Recuperate occurences of the 2 archetypes
            int occurence1 = deckOccurences.getOrDefault(archetype1, 0);
            int occurence2 = deckOccurences.getOrDefault(archetype2, 0);

            // Calculate
            double stat = (double) occurence1 * occurence2 / (double) (nbDeck);
            // Get stat with only 2 decimals
            stat = Math.round(stat * 100.0) / 100.0;

            context.write(new Text(archetype1 + ';' + archetype2 + ";" + totalGames + ";" + totalWins + ";" + occurence1
                    + ";" + occurence2 + ";" + stat), NullWritable.get());
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
            System.err.println("Usage: <input path> <output path> <archetype file>");
            return -1;
        }
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "Clash Royale Deck Popularity");
        job.addCacheFile(new URI(args[2]));

        job.setJarByClass(DeckGlobalStatsJob.class);

        job.setMapperClass(DeckGlobalStatsMapper.class);
        job.setReducerClass(DeckGlobalStatsReducer.class);

        // --- FIX START ---
        // Explicitly define Mapper output types because they differ from Reducer output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // --- FIX END ---

        // Define Reducer (Final) output types
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