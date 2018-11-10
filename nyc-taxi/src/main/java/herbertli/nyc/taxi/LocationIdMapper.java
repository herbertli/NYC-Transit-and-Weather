package herbertli.nyc.taxi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LocationIdMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] split_line = value.toString().split(",");
        IntWritable locationId = new IntWritable(Integer.parseInt(split_line[0]));
        context.write(locationId, one);
    }
}
