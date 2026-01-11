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

public class DeckToArchetypeCode extends Configured implements Tool {

    // Mapper : Lit "DeckA-DeckB", émet (DeckA, 1) et (DeckB, 1)
    public static class DeckToArchetypeCodeMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

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

            for (String w : wSubs) {
                context.write(new Text(w), NullWritable.get());
            }
            for (String l : lSubs) {
                context.write(new Text(l), NullWritable.get());
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
    public static class DeckToArchetypeCodeReducer extends Reducer<Text, NullWritable, NullWritable, Text> {
        private int minThreshold = 0;
        private int index = 0;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            this.minThreshold = conf.getInt("filter.threshold", 10000);
            this.index = 0;
        }

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            for (NullWritable val : values) {
                count++;
            }
            if (count >= minThreshold) {
                index++;
                String output = String.format("%s,%d", key.toString(), index);
                context.write(NullWritable.get(), new Text(output));
            }
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

        job.setMapperClass(DeckToArchetypeCodeMapper.class);
        job.setReducerClass(DeckToArchetypeCodeReducer.class);

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
        int res = ToolRunner.run(new Configuration(), new DeckToArchetypeCode(), args);
        System.exit(res);
    }

}