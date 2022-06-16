import job.DayFrequencyJob;
import job.LargestDayPriceJob;
import job.OrderCounterJob;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

public class Program {
    public static void main(String[] args) {
        try {
            FileUtils.deleteDirectory(new File("output"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            ToolRunner.run(new OrderCounterJob(), args);
            ToolRunner.run(new DayFrequencyJob(), args);
            ToolRunner.run(new LargestDayPriceJob(), args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
