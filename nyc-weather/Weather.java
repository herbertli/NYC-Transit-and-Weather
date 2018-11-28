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

public class Weather {

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

            String month = null;
            if (line.charAt(106) == '0')
                month = line.substring(107, 108);
            else
                month = line.substring(106, 108);

            String day = null;
            if (line.charAt(108) == '0')
                day = line.substring(109, 110);
            else
                day = line.substring(108, 110);

            // PRIMARY FIELDS

            String prcp = line.substring(111, 119).trim();
            String prcpQuality = line.substring(137, 149).trim();
            if (!valid(prcp, prcpQuality))
                prcp = "null";

            String snwd = line.substring(182, 190).trim();
            String snwdQuality = line.substring(208, 220).trim();
            if (!valid(snwd, snwdQuality))
                snwd = "null";

            String snow = line.substring(253, 261).trim();
            String snowQuality = line.substring(279, 291).trim();
            if (!valid(snow, snowQuality))
                snow = "null";

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

            String s = String.format("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s", year, month, day, prcp, snwd, snow, tavg, tmax, tmin, awnd, fog, wt03, hail, wt08, wt09);
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

    public static class WeatherProfilerMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(0), value);
        }
    }

    public static class WeatherProfilerReducer extends Reducer<LongWritable, Text, Text, Text> {

        private static double stdev(List<Double> values, int n, double avg) {
            double sumOfSquares = 0;
            for (double value : values) {
                sumOfSquares += Math.pow(value - avg, 2);
            }
            return Math.sqrt(sumOfSquares / n);
        }

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // initialize

            int records = 0;

            List<Double> prcpValues = new LinkedList<Double>();
            double prcpSum = 0;
            double prcpMax = Double.MIN_VALUE;
            double prcpMin = Double.MAX_VALUE;

            List<Double> snwdValues = new LinkedList<Double>();
            double snwdSum = 0;
            double snwdMax = Double.MIN_VALUE;
            double snwdMin = Double.MAX_VALUE;

            List<Double> snowValues = new LinkedList<Double>();
            double snowSum = 0;
            double snowMax = Double.MIN_VALUE;
            double snowMin = Double.MAX_VALUE;

            List<Double> tavgValues = new LinkedList<Double>();
            double tavgSum = 0;
            double tavgMax = Double.MIN_VALUE;
            double tavgMin = Double.MAX_VALUE;

            List<Double> tmaxValues = new LinkedList<Double>();
            double tmaxSum = 0;
            double tmaxMax = Double.MIN_VALUE;
            double tmaxMin = Double.MAX_VALUE;

            List<Double> tminValues = new LinkedList<Double>();
            double tminSum = 0;
            double tminMax = Double.MIN_VALUE;
            double tminMin = Double.MAX_VALUE;

            List<Double> awndValues = new LinkedList<Double>();
            double awndSum = 0;
            double awndMax = Double.MIN_VALUE;
            double awndMin = Double.MAX_VALUE;

            int fogCount = 0;
            int thunderCount = 0;
            int hailCount = 0;
            int hazeCount = 0;

            for (Text text : values) {

                records += 1;

                // get fields
                String s = text.toString();
                String[] fields = s.split(",");

                double prcp = Double.parseDouble(fields[3].trim());
                prcpValues.add(prcp);
                prcpSum += prcp;
                if (prcp > prcpMax)
                    prcpMax = prcp;
                if (prcp < prcpMin)
                    prcpMin = prcp;

                double snwd = Double.parseDouble(fields[4].trim());
                snwdValues.add(snwd);
                snwdSum += snwd;
                if (snwd > snwdMax)
                    snwdMax = snwd;
                if (snwd < snwdMin)
                    snwdMin = snwd;

                double snow = Double.parseDouble(fields[5].trim());
                snowValues.add(snow);
                snowSum += snow;
                if (snow > snowMax)
                    snowMax = snow;
                if (snow < snowMin)
                    snowMin = snow;

                double tavg = Double.parseDouble(fields[6].trim());
                tavgValues.add(tavg);
                tavgSum += tavg;
                if (tavg > tavgMax)
                    tavgMax = tavg;
                if (tavg < tavgMin)
                    tavgMin = tavg;

                double tmax = Double.parseDouble(fields[7].trim());
                tmaxValues.add(tmax);
                tmaxSum += tmax;
                if (tmax > tmaxMax)
                    tmaxMax = tmax;
                if (tmax < tmaxMin)
                    tmaxMin = tmax;

                double tmin = Double.parseDouble(fields[8].trim());
                tminValues.add(tmin);
                tminSum += tmin;
                if (tmin > tminMax)
                    tminMax = tmin;
                if (tmin < tminMin)
                    tminMin = tmin;

                double awnd = Double.parseDouble(fields[9].trim());
                awndValues.add(awnd);
                awndSum += awnd;
                if (awnd > awndMax)
                    awndMax = awnd;
                if (awnd < awndMin)
                    awndMin = awnd;

                String fog = fields[10].trim();
                if (fog.equals("1"))
                    fogCount += 1;

                String thunder = fields[11].trim();
                if (thunder.equals("1"))
                    thunderCount += 1;

                String hail = fields[12].trim();
                if (hail.equals("1"))
                    hailCount += 1;

                String haze = fields[13].trim();
                if (haze.equals("1"))
                    hazeCount += 1;
            }

            double prcpAvg = prcpSum / records;
            double snwdAvg = snwdSum / records;
            double snowAvg = snowSum / records;
            double tavgAvg = tavgSum / records;
            double tmaxAvg = tmaxSum / records;
            double tminAvg = tminSum / records;
            double awndAvg = awndSum / records;

            double prcpStdev = stdev(prcpValues, records, prcpAvg);
            double snwdStdev = stdev(snwdValues, records, snwdAvg);
            double snowStdev = stdev(snowValues, records, snowAvg);
            double tavgStdev = stdev(tavgValues, records, tavgAvg);
            double tmaxStdev = stdev(tmaxValues, records, tmaxAvg);
            double tminStdev = stdev(tminValues, records, tminAvg);
            double awndStdev = stdev(awndValues, records, awndAvg);

            context.write(new Text("prcp"), new Text(String.format("type=double, max=%f, min=%f, avg=%f, stdev=%f", prcpMax, prcpMin, prcpAvg, prcpStdev)));
            context.write(new Text("snwd"), new Text(String.format("type=double, max=%f, min=%f, avg=%f, stdev=%f", snwdMax, snwdMin, snwdAvg, snwdStdev)));
            context.write(new Text("snow"), new Text(String.format("type=double, max=%f, min=%f, avg=%f, stdev=%f", snowMax, snowMin, snowAvg, snowStdev)));
            context.write(new Text("tavg"), new Text(String.format("type=double, max=%f, min=%f, avg=%f, stdev=%f", tavgMax, tavgMin, tavgAvg, tavgStdev)));
            context.write(new Text("tmax"), new Text(String.format("type=double, max=%f, min=%f, avg=%f, stdev=%f", tmaxMax, tmaxMin, tmaxAvg, tmaxStdev)));
            context.write(new Text("tmin"), new Text(String.format("type=double, max=%f, min=%f, avg=%f, stdev=%f", tminMax, tminMin, tminAvg, tminStdev)));
            context.write(new Text("awnd"), new Text(String.format("type=double, max=%f, min=%f, avg=%f, stdev=%f", awndMax, awndMin, awndAvg, awndStdev)));
            context.write(new Text("fog"), new Text(String.format("type=boolean, occurrences=%d", fogCount)));
            context.write(new Text("thunder"), new Text(String.format("type=boolean, occurrences=%d", thunderCount)));
            context.write(new Text("hail"), new Text(String.format("type=boolean, occurrences=%d", hailCount)));
            context.write(new Text("haze"), new Text(String.format("type=boolean, occurrences=%d", hazeCount)));
        }
    }

    public static void main(String[] args) throws Exception {

        // validate command line args
        if (args.length != 3) {
            System.err.println("Usage: Weather <input path> <cleaner output path> <profiler output path>");
            System.exit(1);
        }

        // Cleaner

        Job job = new Job();
        job.setJarByClass(Weather.class);
        job.setJobName("WeatherCleaner");
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WeatherCleanerMapper.class);
        job.setReducerClass(WeatherCleanerReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        // Profiler

        job = new Job();
        job.setJarByClass(Weather.class);
        job.setJobName("WeatherProfiler");
        
        FileInputFormat.addInputPath(job, new Path(args[1] + "/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapperClass(WeatherProfilerMapper.class);
        job.setReducerClass(WeatherProfilerReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}