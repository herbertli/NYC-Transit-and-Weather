import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer
    extends Reducer<LongWritable, Text, LongWritable, Text> {

  @Override
  public void reduce(LongWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    Text a = null;
    for (Text text : values) {
      a = text;
      break;
    }

    context.write(null, a);
  }
}