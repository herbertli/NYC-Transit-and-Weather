package herbertli.nyc.taxi.old;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DateTimeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static final String TIMESTAMP_PATTERN = "dd/MMM/yyyy:HH:mm:ss";

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TIMESTAMP_PATTERN);
        LocalDate localDate = LocalDate.parse(key.toString(), formatter);
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        IntWritable result = new IntWritable(sum);
        context.write(new Text(localDate.toString()), result);
    }
}