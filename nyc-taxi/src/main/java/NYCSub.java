import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.TreeMap;


public class NYCSub {

    public static class TurnMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final String TIMESTAMP_PATTERN = "MM/dd/yyyy HH:mm:ss";
        private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TIMESTAMP_PATTERN);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            if (split.length != 2) return;

            String counter = split[1];
            String[] splitK = split[0].split(";");

            if (splitK.length != 4) return;

            String date = splitK[0];
            String time = splitK[3];
            String datetime = date + " " + time;
            LocalDateTime parsed = LocalDateTime.parse(datetime, formatter);
            String station = splitK[1];
            String line = splitK[2];

            String newKey = String.join(",", station, line);
            String newValue = String.join(",", parsed.toString(), counter);
            context.write(new Text(newKey), new Text(newValue));
        }

    }

    public static class TurnReducer extends Reducer<Text, Text, Text, LongWritable> {
        private static final String TIMESTAMP_PATTERN = "yyyy-MM-ddTHH:mm";
        private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TIMESTAMP_PATTERN);

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TreeMap<LocalDateTime, Long> m = new TreeMap<>();
            for (Text value: values) {
                String[] split = value.toString().split(",");
                long counter = Long.parseLong(split[1]);
                String datetime1 = split[0];
                LocalDateTime parsed = LocalDateTime.parse(datetime1, formatter);
                if (parsed.getMinute() == 0) {
                    parsed = parsed.withSecond(0);
                } else {
                    parsed = parsed.plusHours(1).withSecond(0);
                }
                if (!m.containsKey(parsed)) {
                    m.put(parsed, counter);
                } else {
                    if (m.get(parsed) < counter) {
                        m.put(parsed, counter);
                    }
                }
            }

            Map.Entry<LocalDateTime, Long> prevEntry = null;
            for (Map.Entry<LocalDateTime, Long> e: m.entrySet()) {
                if (prevEntry != null) {
                    long hoursBetween = ChronoUnit.HOURS.between(prevEntry.getKey(), e.getKey());
                    if (hoursBetween > 3 && hoursBetween < 5) {
                        String newKey = String.join(",", prevEntry.getKey().toString(), e.getKey().toString(), key.toString());
                        context.write(new Text(newKey), new LongWritable(e.getValue() - prevEntry.getValue()));
                    }
                }
                prevEntry = e;
            }
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: NYCSub <input_path> <output_path>");
            System.exit(0);
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "analyzing nyc subway data");
        job.setJarByClass(NYCSub.class);
        job.setMapperClass(TurnMapper.class);
        job.setReducerClass(TurnReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
