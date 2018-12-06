import org.apache.commons.lang.ArrayUtils;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class DataConsolidate {

    public static class DataMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private static DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            LocalDateTime pu_t = LocalDateTime.parse(split[0], formatter);
            if (pu_t.getMinute() > 0 || pu_t.getSecond() > 0) {
                pu_t = pu_t.plusHours(1).withMinute(0).withSecond(0);
            }
            LocalDateTime do_t = LocalDateTime.parse(split[1], formatter);
            if (do_t.getMinute() > 0 || do_t.getSecond() > 0) {
                do_t = do_t.plusHours(1).withMinute(0).withSecond(0);
            }
            long pass_n = Long.parseLong(split[9]);
            split[0] = pu_t.toString();
            split[1] = do_t.toString();
            StringBuilder newKey = new StringBuilder();
            newKey.append(split[0]);
            for (int i = 1; i < split.length; i++) {
                if (i == 9) {
                    continue;
                }
                newKey.append(",").append(split[i]);
            }
            context.write(new Text(newKey.toString()), new LongWritable(pass_n));
        }

    }

    public static class DataReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: DataConsolidate <input_path> <output_path>");
            System.exit(0);
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "analyzing nyc taxi data");
        job.setJarByClass(DataConsolidate.class);
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);

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
