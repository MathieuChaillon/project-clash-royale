import java.io.IOException;
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

public class ArchetypeStatsJob extends Configured implements Tool {

    // Mapper : Lit "DeckA-DeckB", émet (DeckA, 1) et (DeckB, 1)
    public static class StatsMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final String NODE_PREFIX = "N:";
        private static final String EDGE_PREFIX = "E:";

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // On s'attend à une entrée formatée : "deckA-deckB"
            String line = value.toString();
            String[] decks = line.split("-");

            if (decks.length != 2) {
                return;
            }

            // Generate 70 subdecks for each deck
            List<String> wSubs = generateSubDecks(decks[0]);
            List<String> lSubs = generateSubDecks(decks[1]);

            String winPrefix = "W:";
            String losePrefix = "L:";
            String allWinners = winPrefix + String.join(",", wSubs);
            String allLosers = losePrefix + String.join(",", lSubs);

            // 2. Émettre pour les ARÊTES (Edges)
            for (String w : wSubs) {
                context.write(new Text("E:" + w), new Text(allLosers));
            }
            for (String l : lSubs) {
                context.write(new Text("E:" + l), new Text(allWinners));
            }

            // 3. Émettre pour les NOEUDS (Nodes) - Ça ne change pas ou peu
            // On peut optimiser en émettant juste "1" et en comptant au reduce
            for (String w : wSubs) {
                context.write(new Text("N:" + w), new Text("1,1")); // 1 match, 1 victoire
            }
            for (String l : lSubs) {
                context.write(new Text("N:" + l), new Text("1,0")); // 1 match, 0 victoire
            }
        }

        private List<String> generateSubDecks(String fullDeck) {
            List<String> subDecks = new ArrayList<>(70);
            List<String> cards = new ArrayList<>(8);

            // Sécurité de base
            if (fullDeck == null || fullDeck.length() != 16) {
                return subDecks;
            }

            for (int i = 0; i < 16; i += 2) {
                cards.add(fullDeck.substring(i, i + 2));
            }

            Collections.sort(cards);

            int n = cards.size();

            String cardi, cardj, cardk, cardl;

            for (int i = 0; i < n - 3; i++) {
                for (int j = i + 1; j < n - 2; j++) {
                    for (int k = j + 1; k < n - 1; k++) {
                        for (int l = k + 1; l < n; l++) {

                            cardi = cards.get(i);
                            cardj = cards.get(j);
                            cardk = cards.get(k);
                            cardl = cards.get(l);
                            subDecks.add(cardi + cardj + cardk + cardl);
                        }
                    }
                }
            }

            return subDecks;
        }
    }

    // Reducer : Somme les occurrences, filtre et formate
    public static class StatsReducer extends Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mos;
        private int minThreshold = 0;

        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs<NullWritable, Text>(context);
            Configuration conf = context.getConfiguration();
            this.minThreshold = conf.getInt("filter.threshold", 10000);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String keyStr = key.toString();
            if (keyStr.startsWith("N:")) {
                long count = 0;
                long wins = 0;
                for (Text val : values) {
                    String[] parts = val.toString().split(",");
                    count += Long.parseLong(parts[0]);
                    wins += Long.parseLong(parts[1]);
                }
                mos.write("nodes", NullWritable.get(), new Text(keyStr.substring(2) + ";" + count + ";" + wins));

            } else if (keyStr.startsWith("E:")) {
                // --- GESTION DES ARÊTES (Optimisée) ---
                String sourceDeck = keyStr.substring(2);

                Map<String, Integer> targetCounts = new HashMap<>();
                Map<String, Integer> targetWins = new HashMap<>();

                for (Text val : values) {
                    String[] opposite = val.toString().split(",");
                    String prefix = opposite[0];
                    String[] targets = opposite[1].split(",");
                    if (prefix.equals("W:")) {
                        // Decks which won against sourceDeck
                        for (String target : targets) {
                            targetCounts.put(target, targetCounts.getOrDefault(target, 0) + 1);
                        }
                    } else if (prefix.equals("L:")) {
                        // Decks which lost against sourceDeck
                        for (String target : targets) {
                            targetWins.put(target, targetWins.getOrDefault(target, 0) + 1);
                            targetCounts.put(target, targetCounts.getOrDefault(target, 0) + 1);
                        }
                    }
                }

                // Écriture des résultats filtrés
                for (String target : targetCounts.keySet()) {
                    int totalMatches = targetCounts.get(target);
                    if (totalMatches >= minThreshold) {
                        int totalWins = targetWins.getOrDefault(target, 0);
                        mos.write("edges", NullWritable.get(),
                                new Text(sourceDeck + ";" + target + ";" + totalMatches + ";" + totalWins));
                    }
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
        if (args.length < 3) {
            System.err.println("Usage: DeckPopularity <input path> <output path> <min_threshold>");
            return -1;
        }

        Configuration conf = getConf();
        conf.setInt("filter.threshold", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "Clash Royale Deck Popularity");
        job.setJarByClass(ArchetypeStatsJob.class);

        job.setMapperClass(StatsMapper.class);
        job.setReducerClass(StatsReducer.class);

        // --- FIX START ---
        // Explicitly define Mapper output types because they differ from Reducer output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // --- FIX END ---

        // Define Reducer (Final) output types
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleOutputs.addNamedOutput(job, "nodes", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "edges", TextOutputFormat.class, Text.class, Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ArchetypeStatsJob(), args);
        System.exit(res);
    }
}