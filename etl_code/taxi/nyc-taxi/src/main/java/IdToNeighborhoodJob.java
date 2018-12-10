import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class IdToNeighborhoodJob {

    public static class IdMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        HashMap<Integer, String> lookup;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
                URI zoneLookupPath = context.getCacheFiles()[0];
                lookup = readFile(zoneLookupPath, context);
            }
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String sPickup = split[split.length - 3];
            String sDropOff = split[split.length - 2];
            int pass = Integer.parseInt(split[split.length - 1]);

            int pickId = -100;
            if (sPickup.length() > 0 && StringUtils.isNumeric(sPickup)) {
                pickId = Integer.parseInt(sPickup);
            }
            String pickValue = lookup.getOrDefault(pickId, "NULL,NULL");

            int dropId = -100;
            if (sDropOff.length() > 0 && StringUtils.isNumeric(sDropOff)) {
                dropId = Integer.parseInt(sDropOff);
            }
            String dropValue = lookup.getOrDefault(dropId, "NULL,NULL");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < split.length - 1; i++) {
                if (i != 0) sb.append(",");
                sb.append(split[i]);
            }
            String join = String.join(",", sb.toString(), pickValue, dropValue);

            context.write(new Text(join), new LongWritable(pass));
        }

        static HashMap<Integer, String> readFile(URI uri, Context context) throws IOException {
            HashMap<Integer, String> m = new HashMap<>();
            Path path = new Path(uri.getPath());
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = br.readLine()) != null) {
                String[] split = line.split(",");
                if (StringUtils.isNumeric(split[0])) {
                    int id = Integer.parseInt(split[0]);
                    String borough = split[1].replace("\"", "");
                    String zone = split[2].replace("\"", "");
                    m.put(id, String.join(",", borough, zone));
                }
            }
            br.close();
            return m;
        }

    }

    public static class IdReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value: values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage IdToNeighborhoodJob <input_path> <output_path> <csv_file>");
            System.exit(0);
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "analyzing nyc taxi data");
        job.addCacheFile(new Path(args[2]).toUri());

        job.setJarByClass(IdToNeighborhoodJob.class);

        job.setMapperClass(IdMapper.class);
        job.setCombinerClass(IdReducer.class);
        job.setReducerClass(IdReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

