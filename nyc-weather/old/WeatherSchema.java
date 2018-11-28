import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherSchema {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: WeatherSchema <input path> <output path>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(WeatherSchema.class);
    job.setJobName("WeatherSchema");
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(WeatherSchemaMapper.class);
    job.setReducerClass(WeatherSchemaReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}