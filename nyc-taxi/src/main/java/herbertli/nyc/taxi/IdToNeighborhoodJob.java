package herbertli.nyc.taxi;

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

    public static class IdMapper extends Mapper<LongWritable, Text, Text, Text> {

        HashMap<Integer, String> lookup;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
                URI zoneLookupPath = context.getCacheFiles()[0];
                lookup = readFile(zoneLookupPath, context);
            }
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            if (StringUtils.isNumeric(split[2])) {
                int locId = Integer.parseInt(split[2]);
                String locValue = lookup.getOrDefault(locId, "NULL,NULL");
                String originalVal = value.toString();
                originalVal = originalVal.trim();
                if (originalVal.endsWith(",")) originalVal = originalVal.substring(0, originalVal.length() - 1);
                String join = String.join(",", originalVal, locValue);
                context.write(new Text(join), new Text());
            }
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

    public static class IdReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value: values) {
                context.write(key, value);
            }
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
        job.setOutputValueClass(Text.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

