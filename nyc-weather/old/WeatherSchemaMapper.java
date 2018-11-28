import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherSchemaMapper
    extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    // skip first two lines
    if (key.get() == 0 || key.get() == 1532) {
        // do nothing
    } else {
      String line = value.toString();

      String date = line.substring(102, 110).trim();

      String prcp = line.substring(111, 119).trim();
      String snwd = line.substring(182, 190).trim();
      String snow = line.substring(253, 261).trim();
      String tavg = line.substring(324, 332).trim();
      String tmax = line.substring(395, 403).trim();
      String tmin = line.substring(466, 474).trim();

      context.write(new Text("prcp"), new DoubleWritable(Double.parseDouble(prcp)));
      context.write(new Text("snwd"), new DoubleWritable(Double.parseDouble(snwd)));
      context.write(new Text("snow"), new DoubleWritable(Double.parseDouble(snow)));
      context.write(new Text("tavg"), new DoubleWritable(Double.parseDouble(tavg)));
      context.write(new Text("tmax"), new DoubleWritable(Double.parseDouble(tmax)));
      context.write(new Text("tmin"), new DoubleWritable(Double.parseDouble(tmin)));
    } 
  }
}