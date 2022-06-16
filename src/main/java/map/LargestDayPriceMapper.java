package map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

public class LargestDayPriceMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable longWritable, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] tableRow = valueString.split(";");

        try {
            TemporalAccessor ta = DateTimeFormatter.ISO_INSTANT.parse(tableRow[8]);
            Date rawDate = Date.from(Instant.from(ta));
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            String formattedDate = format.format(rawDate);

            int price = Integer.parseInt(tableRow[6]);

            output.collect(new Text(formattedDate), new IntWritable(price));

        } catch (Exception ignored) {}
    }
}
