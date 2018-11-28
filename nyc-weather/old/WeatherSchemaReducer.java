import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherSchemaReducer
    extends Reducer<Text, DoubleWritable, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {

    double min = 999999999;
    double max = -999999999;
    double sum = 0;
    int count = 0;

    for (DoubleWritable value : values) {
      double val = value.get();
      if (val < min) {
        min = val;
      }
      if (val > max) {
        max = val;
      }
      sum += val;
      count += 1;
    }

    double avg = sum / count;

    String s = String.format("Type: double, Max: %f, Min: %f, Avg: %f", max, min, avg);
    context.write(key, new Text(s));
  }
}