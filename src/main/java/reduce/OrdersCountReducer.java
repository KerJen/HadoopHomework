package reduce;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class OrdersCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text phone, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
        int phoneNumberCount = 0;
        while (values.hasNext()) {
            IntWritable value =  values.next();
            phoneNumberCount += value.get();
        }
        output.collect(phone, new IntWritable(phoneNumberCount));
    }
}