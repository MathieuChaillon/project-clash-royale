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

public class GlobalStatsJob extends Configured implements Tool {


    public static class GlobalStatsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private HashMap<String, Integer> archetypeOccurences = new HashMap<>();
        private int nbArchetype = 0;
        private int nbGames = 0;

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
                            String archetype = tokens[0];
                            Integer archetypeOccurence = Integer.parseInt(tokens[1]);
                            nbArchetype += archetypeOccurence;
                            archetypeOccurences.put(archetype, archetypeOccurence);
                        }
                    }
                    rdr.close();
                }
            }
            nbGames = nbArchetype / 28; // Each game involves 2 archetypes, each archetype appears in 28 subdecks
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Read line formatted as : "source;target;totalGames;totalWins"
            String line = value.toString();
            String[] data = line.split(";");
            if (data.length != 4) {
                return; // Ignore malformed lines
            }

            String archetypeSource = data[0];
            String archetypeTarget = data[1];
            int totalGames = Integer.parseInt(data[2]);
            int totalWins = Integer.parseInt(data[3]);

            // Recuperate occurences of the 2 archetypes
            int occurenceSource = archetypeOccurences.getOrDefault(archetypeSource, 0);
            int occurenceTarget = archetypeOccurences.getOrDefault(archetypeTarget, 0);

            // Calculate
            double stat = (double) occurenceSource * occurenceTarget / (double) (nbGames);
            // Get stat with only 2 decimals
            stat = Math.round(stat * 100.0) / 100.0;

            context.write(new Text(archetypeSource + ';' + archetypeTarget + ";" + totalGames + ";" + totalWins + ";" + occurenceSource
                    + ";" + occurenceTarget + ";" + stat), NullWritable.get());
        }

    }

    // Reducer : Simply output the received data
    public static class GlobalStatsReducer extends Reducer<Text, NullWritable, NullWritable, Text> {
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

        job.setJarByClass(GlobalStatsJob.class);

        job.setMapperClass(GlobalStatsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);
        job.setReducerClass(GlobalStatsReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GlobalStatsJob(), args);
        System.exit(res);
    }
}