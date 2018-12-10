package old;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DateTimeFeaturizer {

    public static class FeaturizeMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String TIMESTAMP_PATTERN = "MM/dd/yyyy";
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TIMESTAMP_PATTERN);
            String[] split = value.toString().split(",");
            LocalDate pickupDateTime = LocalDate.parse(split[0], formatter);

            String month = pickupDateTime.getMonthValue() + "";
            // 1 - Monday, 7 - Sunday
            String dayOfWeek = pickupDateTime.getDayOfWeek().getValue() + "";
            String dayOfMonth = pickupDateTime.getDayOfMonth() + "";
            String dayOfYear = pickupDateTime.getDayOfYear() + "";
            Text outKey = new Text(String.join(",", value.toString(), month, dayOfMonth, dayOfWeek, dayOfYear));
            context.write(outKey, new Text(""));
        }
    }

    public static class FeaturizeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value: values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage DateTimeFeaturize <input_path> <output_path>");
            System.exit(0);
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "analyzing nyc taxi data");
        job.setJarByClass(DateTimeFeaturizer.class);

        job.setMapperClass(FeaturizeMapper.class);
        job.setCombinerClass(FeaturizeReducer.class);
        job.setReducerClass(FeaturizeReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
