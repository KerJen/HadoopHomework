import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
public class OrdersSumPerPersonMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable longWritable, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] tableRow = valueString.split(";");

        try {
            String valueString = value.toString();
            String[] SingleCountryData = valueString.split(";");

            int price = Integer.parseInt(SingleCountryData[6]);
            output.collect(new Text(SingleCountryData[2]), new IntWritable(price));

        } catch (Exception ignored) {}
    }
}
