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

public class ArchetypeStatsJob extends Configured implements Tool {

    public enum GameCounters {
        UNKNOWN_ARCHETYPE,
        NB_ARCHETYPES,
        TOTAL_GAMES
    }

    // Mapper : Lit "DeckA-DeckB", émet (DeckA, 1) et (DeckB, 1)
    public static class StatsMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final String NODE_PREFIX = "N:";
        private static final String EDGE_PREFIX = "E:";
        private HashMap<String, String> archetypeCode = new HashMap<>();

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
                        String[] tokens = line.split(",");
                        if (tokens.length >= 2) {
                            String archetypeCodeValue = tokens[1];
                            String archetype = tokens[0];
                            archetypeCode.put(archetype, archetypeCodeValue);
                        }
                    }
                    for (String val : archetypeCode.values()) {
                        context.getCounter(GameCounters.NB_ARCHETYPES).increment(1);
                    }
                    rdr.close();
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // On s'attend à une entrée formatée : "deckA-deckB"
            String line = value.toString();
            String[] decks = line.split("-");

            if (decks.length != 2) {
                return;
            }

            // Generate 28 subdecks for each deck
            List<String> wSubs = generateSubDecks(decks[0]);
            List<String> lSubs = generateSubDecks(decks[1]);

            List<String> wArchetypes = new ArrayList<>();
            List<String> lArchetypes = new ArrayList<>();
            for (String w : wSubs) {
                String archetype = archetypeCode.getOrDefault(w, "UNKNOWN");
                if (!archetype.equals("UNKNOWN")) {
                    wArchetypes.add(archetype);
                } else {
                    context.getCounter(GameCounters.UNKNOWN_ARCHETYPE).increment(1);
                }
            }
            for (String l : lSubs) {
                String archetype = archetypeCode.getOrDefault(l, "UNKNOWN");
                if (!archetype.equals("UNKNOWN")) {
                    lArchetypes.add(archetype);
                } else {
                    context.getCounter(GameCounters.UNKNOWN_ARCHETYPE).increment(1);
                }
            }
            if (wArchetypes.isEmpty() || lArchetypes.isEmpty()) {
                return;
            }
            context.getCounter(GameCounters.TOTAL_GAMES).increment(1);
            String winPrefix = "W:";
            String losePrefix = "L:";
            String allWinners = winPrefix + String.join(",", wArchetypes);
            String allLosers = losePrefix + String.join(",", lArchetypes);

            // 2. Émettre pour les ARÊTES (Edges)
            for (String w : wArchetypes) {
                context.write(new Text(EDGE_PREFIX + w), new Text(allLosers));
            }
            for (String l : lArchetypes) {
                context.write(new Text(EDGE_PREFIX + l), new Text(allWinners));
            }

            // 3. Émettre pour les NOEUDS (Nodes) - Ça ne change pas ou peu
            // On peut optimiser en émettant juste "1" et en comptant au reduce
            for (String w : wArchetypes) {
                context.write(new Text(NODE_PREFIX + w), new Text("1,1")); // 1 match, 1 victoire
            }
            for (String l : lArchetypes) {
                context.write(new Text(NODE_PREFIX + l), new Text("1,0")); // 1 match, 0 victoire
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

            String cardi, cardj, cardk, cardl, cardm, cardo;

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

    // Reducer : Somme les occurrences, filtre et formate
    public static class StatsReducer extends Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mos;

        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs<NullWritable, Text>(context);
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
                    String[] opposite = val.toString().split(":");
                    String prefix = opposite[0];
                    String[] targets = opposite[1].split(",");
                    if (prefix.equals("W")) {
                        // Decks which won against sourceDeck
                        for (String target : targets) {
                            targetCounts.put(target, targetCounts.getOrDefault(target, 0) + 1);
                        }
                    } else if (prefix.equals("L")) {
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
                    int totalWins = targetWins.getOrDefault(target, 0);
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
        if (args.length < 3) {
            System.err.println("Usage: DeckPopularity <input path> <output path> <archetype file>");
            return -1;
        }

        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "Clash Royale Deck Popularity");
        job.addCacheFile(new URI(args[2]));

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