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
import org.apache.hadoop.io.BooleanWritable;
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

public class ListDeckConfrontation extends Configured implements Tool {

    public static class ListDeckConfrontationMapper extends Mapper<LongWritable, Text, Text, BooleanWritable> {

        private HashMap<String, String> codeDeck = new HashMap<>();

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
                        if (tokens.length >= 4) {
                            String deckCode = tokens[0];
                            String deck = tokens[1];
                            codeDeck.put(deck, deckCode);
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

            if (decks.length == 2) {
                String deckA = codeDeck.getOrDefault(decks[0], "UNKNOWN");
                String deckB = codeDeck.getOrDefault(decks[1], "UNKNOWN");

                if (deckA.equals("UNKNOWN") || deckB.equals("UNKNOWN")) {
                    return; // Skip unknown decks
                }
                String confrontationWin = deckA + "," + deckB;
                context.write(new Text(confrontationWin), new BooleanWritable(true));
            
                String confrontationLose = deckB + "," + deckA;
                context.write(new Text(confrontationLose), new BooleanWritable(false));
            }
        }
    }

    // Reducer : Somme les occurrences, filtre et formate
    public static class CountConfrontationReducer extends Reducer<Text, BooleanWritable, NullWritable, Text> {


        @Override
        public void reduce(Text key, Iterable<BooleanWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            int nbWin = 0;

            for (BooleanWritable val : values) {
                if (val.get()) {
                    nbWin++;
                }
                count++;
            }

            String output = String.format("%s,%d,%d", key.toString(), count, nbWin);

            context.write(NullWritable.get(), new Text(output));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: DeckPopularity <input path> <output path> <deck_mapping_file>");
            return -1;
        }

        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "Clash Royale Deck Popularity");
        job.setJarByClass(ListDeckConfrontation.class);
        job.addCacheFile(new URI(args[2]));

        job.setMapperClass(ListDeckConfrontationMapper.class);
        job.setReducerClass(CountConfrontationReducer.class);

        // --- FIX START ---
        // Explicitly define Mapper output types because they differ from Reducer output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BooleanWritable.class);
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
        int res = ToolRunner.run(new Configuration(), new ListDeckConfrontation(), args);
        System.exit(res);
    }
}
