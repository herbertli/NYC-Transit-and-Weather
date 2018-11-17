package herbertli.nyc.taxi;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

class LocationTimeMapper {

    private static final String TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TIMESTAMP_PATTERN);

    static class TaxiMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split_line = value.toString().split(",");
            LocalDateTime pickupTime;

            // check for header rows...
            try {
                pickupTime = LocalDateTime.parse(split_line[0], formatter);
            } catch (DateTimeParseException e) {
                return;
            }

            int dayOfMonth = pickupTime.getDayOfMonth();
            int year = pickupTime.getYear();
            int month = pickupTime.getMonthValue();
            int hourOfDay = pickupTime.getHour();
            int minuteOfHour = pickupTime.getMinute();

            int numPassenger;
            if (StringUtils.isNumeric(split_line[2]))
                numPassenger = Integer.parseInt(split_line[2]);
            else
                numPassenger = 1;

            int pickupLoc = Integer.parseInt(split_line[4]);

            String date = String.format("%02d/%02d/%04d", month, dayOfMonth, year);
            String time = String.format("%02d:%02d", hourOfDay, minuteOfHour);
            String outKey = String.join(",", date, time, "" + pickupLoc);
            context.write(new Text(outKey), new LongWritable(numPassenger));
        }
    }

    static class FHVMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split_line = value.toString().split(",");
            if (split_line.length < 3) return;

            LocalDateTime pickupTime;
            try {
                pickupTime = LocalDateTime.parse(split_line[1], formatter);
            } catch (DateTimeParseException e) {
                return;
            }

            int dayOfMonth = pickupTime.getDayOfMonth();
            int year = pickupTime.getYear();
            int month = pickupTime.getMonthValue();
            int hourOfDay = pickupTime.getHour();
            int minuteOfHour = pickupTime.getMinute();

            int pickupLoc;
            if (!StringUtils.isNumeric(split_line[2]) || split_line[2].trim().length() == 0) {
                pickupLoc = -100;
            } else {
                pickupLoc = Integer.parseInt(split_line[2]);
            }

            String date = String.format("%02d/%02d/%04d", month, dayOfMonth, year);
            String time = String.format("%02d:%02d", hourOfDay, minuteOfHour);
            String outKey;
            if (pickupLoc == -100) {
                outKey = String.join(",", date, time, "null");
            } else {
                outKey = String.join(",", date, time, "" + pickupLoc);
            }

            context.write(new Text(outKey), new LongWritable(1));

        }
    }

}
