import java.io.IOException;
import java.time.Instant;

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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FilterGame extends Configured implements Tool {

    // Définition des groupes de compteurs
    public enum GameCounters {
        INVALID_JSON,
        DUPLICATE_GAMES
    }

    public static class FilterGamesMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final ObjectMapper mapper = new ObjectMapper();
        private static final long TIME_THRESHOLD_MS = 30000;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JsonNode root = mapper.readTree(value.toString());

                // Validation de la structure
                if (!root.has("players") || !root.has("date") || root.get("players").size() != 2) {
                    context.getCounter(GameCounters.INVALID_JSON).increment(1);
                    return;
                }

                JsonNode players = root.get("players");
                String deck0 = players.get(0).get("deck").asText("");
                String deck1 = players.get(1).get("deck").asText("");

                // Validation des decks
                if (deck0.length() != 16 || deck1.length() != 16) {
                    context.getCounter(GameCounters.INVALID_JSON).increment(1);
                    return;
                }

                // Clé de déduplication
                String tag0 = players.get(0).get("utag").asText("");
                String tag1 = players.get(1).get("utag").asText("");
                String playerPair = (tag0.compareTo(tag1) < 0) ? tag0 + tag1 : tag1 + tag0;

                long timestamp = Instant.parse(root.get("date").asText()).toEpochMilli();
                long dateKey = (timestamp / TIME_THRESHOLD_MS) * TIME_THRESHOLD_MS;
                int round = root.get("round").asInt(0);

                String deduplicationKey = dateKey + "_" + playerPair + "_" + round;
                context.write(new Text(deduplicationKey), value);

            } catch (Exception e) {
                // Erreur de parsing JSON
                context.getCounter(GameCounters.INVALID_JSON).increment(1);
            }
        }
    }

    public static class DeduplicationReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean firstSeen = false;
            
            for (Text val : values) {
                if (!firstSeen) {
                    context.write(NullWritable.get(), val);
                    firstSeen = true;
                } else {
                    // Toutes les valeurs suivantes pour cette clé sont des doublons
                    context.getCounter(GameCounters.DUPLICATE_GAMES).increment(1);
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: FilterGame <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Clash Royale Cleaning with Counters");
        job.setJarByClass(FilterGame.class);

        job.setMapperClass(FilterGamesMapper.class);
        job.setReducerClass(DeduplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FilterGame(), args);
        System.exit(res);
    }
}