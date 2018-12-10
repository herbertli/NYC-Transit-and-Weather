import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherCleaner {

    // Input (key, value) = (line offset, line)
    // Output (key, value) = (line offset, parsed csv data)
    public static class WeatherCleanerMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        // PRIMARY FIELDS - check if the value is valid
        private static boolean valid(String value, String qualityFlag) {
            return qualityFlag.equals("") && !value.equals("9999") && !value.equals("-9999");
        }

        // SECONDARY FIELDS - check if the value is valid
        private static String valid2(String value, String qualityFlag) {
            if (!qualityFlag.equals("")) {
                return "null";
            } else if (value.equals("-9999") || value.equals("9999")) {
                return "0";
            } else {
                return "1";
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            // first 2 lines of input file are metadata - ignore them
            if (line.charAt(0) == 'S' || line.charAt(0) == '-')
                return;

            // get year, month, day
            String year = line.substring(102, 106);
            String month = line.substring(106, 108);
            String day = line.substring(108, 110);

            // PRIMARY FIELDS

            String prcp = line.substring(111, 119).trim();
            String prcpQuality = line.substring(137, 149).trim();
            if (!valid(prcp, prcpQuality))
                prcp = "null";

            String prcpPresence = "";
            if (prcp.equals("null"))
                prcpPresence = "null";
            else if (Double.parseDouble(prcp) < 0.01)
                prcpPresence = "0";
            else
                prcpPresence = "1";

            String snwd = line.substring(182, 190).trim();
            String snwdQuality = line.substring(208, 220).trim();
            if (!valid(snwd, snwdQuality))
                snwd = "null";

            String snow = line.substring(253, 261).trim();
            String snowQuality = line.substring(279, 291).trim();
            if (!valid(snow, snowQuality))
                snow = "null";

            String snowPresence = "";
            if (snow.equals("null"))
                snowPresence = "null";
            else if (Double.parseDouble(snow) < 0.01)
                snowPresence = "0";
            else
                snowPresence = "1";

            String tavg = line.substring(324, 332).trim();
            String tavgQuality = line.substring(350, 362).trim();
            if (!valid(tavg, tavgQuality))
                tavg = "null";

            String tmax = line.substring(395, 403).trim();
            String tmaxQuality = line.substring(421, 433).trim();
            if (!valid(tmax, tmaxQuality))
                tmax = "null";

            String tmin = line.substring(466, 474).trim();
            String tminQuality = line.substring(492, 504).trim();
            if (!valid(tmin, tminQuality))
                tmin = "null";

            // average daily wind speed (mph)
            String awnd = line.substring(537, 545).trim();
            String awndQuality = line.substring(563, 575).trim();
            if (!valid(awnd, awndQuality))
                awnd = "null";

            // SECONDARY FIELDS

            // Fog, ice fog, or freezing fog (may include heavy fog)
            String wt01 = line.substring(1034, 1042).trim();
            String wt01Quality = line.substring(1060, 1072).trim();
            wt01 = valid2(wt01, wt01Quality);

            // Heavy fog or heaving freezing fog (not always distinguished from fog)
            String wt02 = line.substring(1247, 1255).trim();
            String wt02Quality = line.substring(1273, 1285).trim();
            wt02 = valid2(wt02, wt02Quality);

            String fog = null;
            if (wt01.equals("1") || wt02.equals("1"))
                fog = "1";
            else if (wt01.equals("null") || wt02.equals("null"))
                fog = "null";
            else
                fog = "0";

            // Thunder
            String wt03 = line.substring(1460, 1468).trim();
            String wt03Quality = line.substring(1486, 1498).trim();
            wt03 = valid2(wt03, wt03Quality);

            // Ice pellets, sleet, snow pellets, or small hail 
            String wt04 = line.substring(1318, 1326).trim();
            String wt04Quality = line.substring(1344, 1356).trim();
            wt04 = valid2(wt04, wt04Quality);

            // Hail (may include small hail)
            String wt05 = line.substring(1176, 1184).trim();
            String wt05Quality = line.substring(1202, 1214).trim();
            wt05 = valid2(wt05, wt05Quality);

            String hail = null;
            if (wt04.equals("1") || wt05.equals("1"))
                hail = "1";
            else if (wt04.equals("null") || wt05.equals("null"))
                hail = "null";
            else
                hail = "0";

            // Smoke or haze 
            String wt08 = line.substring(1389, 1397).trim();
            String wt08Quality = line.substring(1415, 1427).trim();
            wt08 = valid2(wt08, wt08Quality);

            // Blowing or drifting snow
            String wt09 = line.substring(963, 971).trim();
            String wt09Quality = line.substring(989, 1001).trim();
            wt09 = valid2(wt09, wt09Quality);

            String s = String.format("%s/%s/%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s", month, day, year, prcp, prcpPresence, snwd, snow, snowPresence, tavg, tmax, tmin, awnd, fog, wt03, hail, wt08, wt09);
            context.write(key, new Text(s));
        }
    }

    // Input (key, value) = (line offset, parsed csv data)
    // Output (key, value) = (null, parsed csv data)
    public static class WeatherCleanerReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(null, text);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // validate command line args
        if (args.length != 2) {
            System.err.println("Usage: WeatherCleaner <input path> <output path>");
            System.exit(1);
        }

        Job job = new Job();
        job.setJarByClass(WeatherCleaner.class);
        job.setJobName("WeatherCleaner");
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WeatherCleanerMapper.class);
        job.setReducerClass(WeatherCleanerReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}