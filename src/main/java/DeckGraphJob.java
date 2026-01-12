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

public class DeckGraphJob extends Configured implements Tool {

    // Mapper : Read deck file, link deck with their code, emit nodes and edges
    public static class DeckGraphMapper extends Mapper<LongWritable, Text, Text, BooleanWritable> {

        private static final String NODE_PREFIX = "N:";
        private static final String EDGE_PREFIX = "E:";
        private HashMap<String, String> deckCodes = new HashMap<>();

        // Load deck code mapping from distributed cache
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
            // Read line of format: WIN_DECK-LOSE_DECK
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

            // Emit edge and node information
            context.write(new Text(EDGE_PREFIX + deckWCode + ";" + deckLCode), new BooleanWritable(true));
            context.write(new Text(EDGE_PREFIX + deckLCode + ";" + deckWCode), new BooleanWritable(false));

            context.write(new Text(NODE_PREFIX + deckWCode), new BooleanWritable(true));
            context.write(new Text(NODE_PREFIX + deckLCode), new BooleanWritable(false));
        }

    }

    // Reducer : Aggregate nodes and edges, count games and wins
    public static class DeckGraphReducer extends Reducer<Text, BooleanWritable, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mos;

        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs<NullWritable, Text>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<BooleanWritable> values, Context context)
                throws IOException, InterruptedException {

            // Retrieve the key type (node or edge)
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
                // write node info
                mos.write("nodes", NullWritable.get(), new Text(keyStr.substring(2) + ";" + count + ";" + wins));

            } else if (keyStr.startsWith("E:")) {
                String edge = keyStr.substring(2);
                int count = 0;
                int win = 0;

                for (BooleanWritable val : values) {
                    count++;
                    if (val.get()) {
                        win++;
                    }
                }
                // write edge info
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

        job.setJarByClass(DeckGraphJob.class);

        job.setMapperClass(DeckGraphMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BooleanWritable.class);

        job.setReducerClass(DeckGraphReducer.class);
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
        int res = ToolRunner.run(new Configuration(), new DeckGraphJob(), args);
        System.exit(res);
    }
}