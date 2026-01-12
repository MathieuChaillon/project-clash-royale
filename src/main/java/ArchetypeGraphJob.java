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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ArchetypeGraphJob extends Configured implements Tool {

    // Mapper : Read archetype file, generate subdecks, emit nodes and edges
    public static class ArchetypeGraphMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final String NODE_PREFIX = "N:";
        private static final String EDGE_PREFIX = "E:";
        private HashMap<String, String> archetypeCode = new HashMap<>();

        // Load archetype code mapping from distributed cache
        public void setup(Context context) throws IOException, InterruptedException {
            URI[] files = context.getCacheFiles(); 

            if (files != null && files.length > 0) {
                for (URI p : files) {
                    File f = new File("./" + new File(p.getPath()).getName());
                    BufferedReader rdr = new BufferedReader(
                            new InputStreamReader(new FileInputStream(f)));
                    String line = null;
                    while ((line = rdr.readLine()) != null) {
                        String[] tokens = line.split(",");
                        if (tokens.length >= 2) {
                            String archetypeCodeValue = tokens[1];
                            String archetype = tokens[0];
                            archetypeCode.put(archetype, archetypeCodeValue);
                        }
                    }
                    rdr.close();
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Read line of format: WIN_DECK-LOSE_DECK
            String line = value.toString();
            String[] decks = line.split("-");

            if (decks.length != 2) {
                return;
            }

            // Generate 28 subdecks (6-card combinations) for each deck
            List<String> wSubs = generateSubDecks(decks[0]);
            List<String> lSubs = generateSubDecks(decks[1]);

            // Link subdecks to archetypes codes
            List<String> wArchetypes = new ArrayList<>();
            List<String> lArchetypes = new ArrayList<>();
            for (String w : wSubs) {
                String archetype = archetypeCode.getOrDefault(w, "UNKNOWN");
                if (!archetype.equals("UNKNOWN")) {
                    wArchetypes.add(archetype);
                }
            }
            for (String l : lSubs) {
                String archetype = archetypeCode.getOrDefault(l, "UNKNOWN");
                if (!archetype.equals("UNKNOWN")) {
                    lArchetypes.add(archetype);
                }
            }

            // If no archetypes found, skip
            if (wArchetypes.isEmpty() || lArchetypes.isEmpty()) {
                return;
            }

            // Create aggregated strings for winners and losers
            String winPrefix = "W:";
            String losePrefix = "L:";
            String allWinners = winPrefix + String.join(",", wArchetypes);
            String allLosers = losePrefix + String.join(",", lArchetypes);

            // Emit edges for all combinations of winner and loser archetypes
            // For optimization, emit aggregated lists
            for (String w : wArchetypes) {
                context.write(new Text(EDGE_PREFIX + w), new Text(allLosers));
            }
            for (String l : lArchetypes) {
                context.write(new Text(EDGE_PREFIX + l), new Text(allWinners));
            }

            // Emit nodes with initial counts
            for (String w : wArchetypes) {
                context.write(new Text(NODE_PREFIX + w), new Text("1,1")); // 1 match, 1 win
            }
            for (String l : lArchetypes) {
                context.write(new Text(NODE_PREFIX + l), new Text("1,0")); // 1 match, 0 win
            }
        }

        // Generate all 6-card combinations (subdecks) from a full 8-card deck
        private List<String> generateSubDecks(String fullDeck) {
            List<String> subDecks = new ArrayList<>(70);
            List<String> cards = new ArrayList<>(8);

            // Security check
            if (fullDeck == null || fullDeck.length() != 16) {
                return subDecks;
            }

            // Extract cards
            for (int i = 0; i < 16; i += 2) {
                cards.add(fullDeck.substring(i, i + 2));
            }

            // Sort cards to ensure consistent combinations
            Collections.sort(cards);

            int n = cards.size();

            String cardi, cardj, cardk, cardl, cardm, cardo;

            // Generate all 6-card combinations from 8 cards
            for (int i = 0; i < n - 5; i++) {
                for (int j = i + 1; j < n - 4; j++) {
                    for (int k = j + 1; k < n - 3; k++) {
                        for (int l = k + 1; l < n - 2; l++) {
                            for (int m = l + 1; m < n - 1; m++) {
                                for (int o = m + 1; o < n; o++) {

                                    cardi = cards.get(i);
                                    cardj = cards.get(j);
                                    cardk = cards.get(k);
                                    cardl = cards.get(l);
                                    cardm = cards.get(m);
                                    cardo = cards.get(o);
                                    subDecks.add(cardi + cardj + cardk + cardl + cardm + cardo);
                                }
                            }
                        }
                    }
                }

            }

            return subDecks;
        }
    }

    // Reducer : Aggregate node and edge statistics
    public static class ArchetypeGraphReducer extends Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mos;
        private int minThreshold = 0;

        @Override
        protected void setup(Context context) {
            // Initialize MultipleOutputs
            mos = new MultipleOutputs<NullWritable, Text>(context);
            //Initilize threshold 
            Configuration conf = context.getConfiguration();
            this.minThreshold = conf.getInt("filter.threshold", 10000);
        

        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Retrieve the key type (node or edge)
            String keyStr = key.toString();

            if (keyStr.startsWith("N:")) {
                long count = 0;
                long wins = 0;
                for (Text val : values) {
                    String[] parts = val.toString().split(",");
                    count += Long.parseLong(parts[0]);
                    wins += Long.parseLong(parts[1]);
                }
                // Write node results
                mos.write("nodes", NullWritable.get(), new Text(keyStr.substring(2) + ";" + count + ";" + wins));

            } else if (keyStr.startsWith("E:")) {

                // Retrieve source deck archetype
                String sourceDeck = keyStr.substring(2);

                // Maps to hold counts and wins against target decks
                Map<String, Integer> targetCounts = new HashMap<>();
                Map<String, Integer> targetWins = new HashMap<>();

                for (Text val : values) {

                    String[] opposite = val.toString().split(":");

                    String prefix = opposite[0]; // "W" or "L"

                    String[] targets = opposite[1].split(",");

                    // Decks which won against sourceDeck
                    if (prefix.equals("W")) {
                        for (String target : targets) {
                            targetCounts.put(target, targetCounts.getOrDefault(target, 0) + 1);
                        }
                    }
                    // Decks which lost against sourceDeck
                    else if (prefix.equals("L")) {
                        for (String target : targets) {
                            targetWins.put(target, targetWins.getOrDefault(target, 0) + 1);
                            targetCounts.put(target, targetCounts.getOrDefault(target, 0) + 1);
                        }
                    }
                }

                // Write edge results and keep only those with more than threshold matches
                for (String target : targetCounts.keySet()) {
                    int totalMatches = targetCounts.get(target);
                    int totalWins = targetWins.getOrDefault(target, 0);
                    if (totalMatches < minThreshold) {
                        continue;
                    }
                    mos.write("edges", NullWritable.get(),
                            new Text(sourceDeck + ";" + target + ";" + totalMatches + ";" + totalWins));

                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: archetypestat <input path> <output path> <archetype_file> <min_threshold>"); 
            return -1;
        }

        Configuration conf = getConf();
        conf.setInt("filter.threshold", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "Clash Royale Archetype Graph Job");
        job.addCacheFile(new URI(args[2]));

        job.setJarByClass(ArchetypeGraphJob.class);

        job.setMapperClass(ArchetypeGraphMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ArchetypeGraphReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Setup MultipleOutputs for nodes and edges
        MultipleOutputs.addNamedOutput(job, "nodes", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "edges", TextOutputFormat.class, Text.class, Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ArchetypeGraphJob(), args);
        System.exit(res);
    }
}