import job.PhoneCounterJob;
import map.OrdersCountMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ToolRunner;
import reduce.OrdersCountReducer;

import java.io.IOException;

public class Program {
    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new PhoneCounterJob(), args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
