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

public class DeckStatsJob extends Configured implements Tool {

    // Mapper : Lit "DeckA-DeckB", émet (DeckA, 1) et (DeckB, 1)
    public static class DeckStatsMapper extends Mapper<LongWritable, Text, Text, BooleanWritable> {

        private static final String NODE_PREFIX = "N:";
        private static final String EDGE_PREFIX = "E:";
        private HashMap<String, String> deckCodes = new HashMap<>();

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
                            String deck = tokens[0];
                            String deckCodeValue = tokens[1];
                            deckCodes.put(deck, deckCodeValue);
                        }
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

            String deckW = decks[0];
            String deckL = decks[1];

            String deckWCode = deckCodes.getOrDefault(deckW, "UNKNOWN");
            String deckLCode = deckCodes.getOrDefault(deckL, "UNKNOWN");

            if (deckWCode.equals("UNKNOWN") || deckLCode.equals("UNKNOWN")) {
                return;
            }

            String winPrefix = "W:";
            String losePrefix = "L:";
            // 2. Émettre pour les ARÊTES (Edges)
            context.write(new Text(EDGE_PREFIX + deckWCode + ";" + deckLCode), new BooleanWritable(true));
            context.write(new Text(EDGE_PREFIX + deckLCode + ";" + deckWCode), new BooleanWritable(false));

            context.write(new Text(NODE_PREFIX + deckWCode), new BooleanWritable(true));
            context.write(new Text(NODE_PREFIX + deckLCode), new BooleanWritable(false));
        }

    }

    // Reducer : Somme les occurrences, filtre et formate
    public static class DeckStatsReducer extends Reducer<Text, BooleanWritable, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mos;

        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs<NullWritable, Text>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<BooleanWritable> values, Context context)
                throws IOException, InterruptedException {

            String keyStr = key.toString();
            if (keyStr.startsWith("N:")) {
                long count = 0;
                long wins = 0;
                for (BooleanWritable val : values) {
                    count++;
                    if (val.get()) {
                        wins++;
                    }
                }
                mos.write("nodes", NullWritable.get(), new Text(keyStr.substring(2) + ";" + count + ";" + wins));

            } else if (keyStr.startsWith("E:")) {
                // --- GESTION DES ARÊTES (Optimisée) ---
                String edge = keyStr.substring(2);
                int count = 0;
                int win = 0;

                for (BooleanWritable val : values) {
                    count++;
                    if (val.get()) {
                        win++;
                    }
                }

                mos.write("edges", NullWritable.get(),
                        new Text(edge + ";" + count + ";" + win));

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

        job.setMapperClass(DeckStatsMapper.class);
        job.setReducerClass(DeckStatsReducer.class);

        // --- FIX START ---
        // Explicitly define Mapper output types because they differ from Reducer output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BooleanWritable.class);
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
        int res = ToolRunner.run(new Configuration(), new DeckStatsJob(), args);
        System.exit(res);
    }
}