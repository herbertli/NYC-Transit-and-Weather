package herbertli.nyc.taxi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataProfiler {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Usage: DataProfiler <input path> <output path> <yellow|green|fhv> <col name>");
            System.exit(0);
        }
        Configuration conf = new Configuration();
        String dataSource = args[2];
        String dataColumn = args[3];
        conf.set("dataSource", dataSource);
        conf.set("dataColumn", dataColumn);
        Job job = Job.getInstance(conf, "Profiling NYC Taxi Data");
        job.setJarByClass(DataProfiler.class);
        job.setMapperClass(LocationIdMapper.class);
        job.setReducerClass(LocationIdReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
