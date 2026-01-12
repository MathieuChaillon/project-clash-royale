import java.io.IOException;

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

public class FilterDeckByOccurences extends Configured implements Tool {

    public static class FilterDeckByOccurencesMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Read line of format: WIN_DECK-LOSE_DECK
            String line = value.toString();
            String[] decks = line.split("-");

            if (decks.length != 2) {
                return;
            }

            context.write(new Text(decks[0]), NullWritable.get());
            context.write(new Text(decks[1]), NullWritable.get());
        }
    }

    public static class FilterDeckByOccurencesReducer extends Reducer<Text, NullWritable, NullWritable, Text> {
        private int minThreshold = 0;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            this.minThreshold = conf.getInt("filter.threshold", 10000);
        }

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (NullWritable val : values) {
                count++;
            }

            if (count >= minThreshold) {
                context.write(NullWritable.get(), key);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: FilterDeckByOccurences <input path> <output path> <min_threshold>");
            return -1;
        }

        Configuration conf = getConf();
        conf.setInt("filter.threshold", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "Clash Royale Cleaning with Counters");
        job.setJarByClass(FilterDeckByOccurences.class);

        job.setMapperClass(FilterDeckByOccurencesMapper.class);
        job.setReducerClass(FilterDeckByOccurencesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FilterDeckByOccurences(), args);
        System.exit(res);
    }
}