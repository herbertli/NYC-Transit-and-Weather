package herbertli.nyc.taxi;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataCleaner {

    public static class ColumnMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String[] rowSplit = value.toString().replace("\"", "").split(",");
            ArrayList<String> rowList = new ArrayList<>();
            switch (conf.get("schema")) {
                case "yellow":
                    rowList = DataSchema.extractYellow(rowSplit);
                    break;
                case "green":
                    rowList = DataSchema.extractGreen(rowSplit);
                    break;
                case "fhv":
                    rowList = DataSchema.extractFHV(rowSplit);
                    break;
            }
            Text outValue = new Text(String.join(",", rowList));
            context.write(outValue, new Text(""));
        }
    }

    public static class ColumnReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value: values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "cleaning nyc taxi data");
        job.setJarByClass(DataCleaner.class);
        job.setMapperClass(ColumnMapper.class);
        job.setCombinerClass(ColumnReducer.class);
        job.setReducerClass(ColumnReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(inputPath.getName(), "cleaned");
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (args[0].contains("yellow")) {
            conf.set("schema", "yellow");
        } else if (args[0].contains("green")) {
            conf.set("schema", "green");
        } else {
            conf.set("schema", "fhv");
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
