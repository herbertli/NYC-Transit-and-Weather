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

public class WeatherProfiler {

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

                double prcp = Double.parseDouble(fields[1].trim());
                prcpValues.add(prcp);
                prcpSum += prcp;
                if (prcp > prcpMax)
                    prcpMax = prcp;
                if (prcp < prcpMin)
                    prcpMin = prcp;

                double snwd = Double.parseDouble(fields[3].trim());
                snwdValues.add(snwd);
                snwdSum += snwd;
                if (snwd > snwdMax)
                    snwdMax = snwd;
                if (snwd < snwdMin)
                    snwdMin = snwd;

                double snow = Double.parseDouble(fields[4].trim());
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
        if (args.length != 2) {
            System.err.println("Usage: WeatherProfiler <input path> <output path>");
            System.exit(1);
        }

        Job job = new Job();
        job.setJarByClass(WeatherProfiler.class);
        job.setJobName("WeatherProfiler");
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

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