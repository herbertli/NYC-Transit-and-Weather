package herbertli.nyc.taxi;

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

import java.io.IOException;

public class DataProfiler {

    public static class ProfileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            int colInd = context.getConfiguration().getInt("colInd", 0);
            String[] rowSplit = value.toString().split(",");
            context.write(new Text(rowSplit[colInd]), new IntWritable(1));
        }
    }

    public static class ProfileReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("colInd", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "Profiling NYC Taxi Data");
        job.setJarByClass(DataProfiler.class);
        job.setMapperClass(ProfileMapper.class);
        job.setCombinerClass(ProfileReducer.class);
        job.setReducerClass(ProfileReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
