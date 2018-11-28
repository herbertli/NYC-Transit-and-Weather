import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper
    extends Mapper<LongWritable, Text, LongWritable, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();

    String date = line.substring(102, 110).trim();

    String prcp = line.substring(111, 119).trim();
    String prcpQuality = line.substring(137, 149).trim();
    if (!prcpQuality.equals(""))
      prcp = "NaN";

    String snwd = line.substring(182, 190).trim();
    String snwdQuality = line.substring(208, 220).trim();
    if (!snwdQuality.equals(""))
      snwd = "NaN";

    String snow = line.substring(253, 261).trim();
    String snowQuality = line.substring(279, 291).trim();
    if (!snowQuality.equals(""))
      snow = "NaN";

    String tavg = line.substring(324, 332).trim();
    String tavgQuality = line.substring(350, 362).trim();
    if (!tavgQuality.equals(""))
      tavg = "NaN";

    String tmax = line.substring(395, 403).trim();
    String tmaxQuality = line.substring(421, 433).trim();
    if (!tmaxQuality.equals(""))
      tmax = "NaN";

    String tmin = line.substring(466, 474).trim();
    String tminQuality = line.substring(492, 504).trim();
    if (!tminQuality.equals(""))
      tmin = "NaN";

    String s = String.format("%s, %s, %s, %s, %s, %s, %s", date, prcp, snwd, snow, tavg, tmax, tmin);
    context.write(key, new Text(s));
  }
}