import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

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

public class FilterGamebyDeck extends Configured implements Tool {

    public static class FilterGamebyDeckMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private List<String> Decks = new ArrayList<>();

        public void setup(Context context) throws IOException, InterruptedException {
            URI[] files = context.getCacheFiles(); 

            if (files != null && files.length > 0) {
                for (URI p : files) {
                    File f = new File("./" + new File(p.getPath()).getName());
                    BufferedReader rdr = new BufferedReader(
                            new InputStreamReader(new FileInputStream(f)));
                    String line = null;
                    while ((line = rdr.readLine()) != null) {
                        Decks.add(line);
                    }
                    rdr.close();
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Read line of format: WIN_DECK-LOSE_DECK
            String line = value.toString();
            String[] decks = line.split("-");


            if (decks.length != 2) {
                return;
            }

            if (!Decks.contains(decks[0]) || !Decks.contains(decks[1])) {
                return;
            }


            context.write(new Text(decks[0] + "-" + decks[1]), NullWritable.get());
        }
    }

    public static class FilterGamebyDeckReducer extends Reducer<Text, NullWritable, NullWritable, Text> {
        
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            
            context.write(NullWritable.get(), key);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: FilterDeckByOccurences <input path> <output path> <min_threshold>");
            return -1;
        }

        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "Clash Royale Cleaning with Counters");
        job.addCacheFile(new URI(args[2]));
        job.setJarByClass(FilterGamebyDeck.class);

        job.setMapperClass(FilterGamebyDeckMapper.class);
        job.setReducerClass(FilterGamebyDeckReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FilterGamebyDeck(), args);
        System.exit(res);
    }
}
