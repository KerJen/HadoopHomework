import map.OrdersCountMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import reduce.OrdersCountReducer;

import java.io.IOException;

public class Program {
    public static void main(String[] args) {
        JobConf job_conf = new JobConf(Program.class);
        job_conf.setJobName("Orders");

        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(IntWritable.class);

        job_conf.setMapperClass(OrdersCountMapper.class);
        job_conf.setReducerClass(OrdersCountReducer.class);

        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job_conf, new Path("input"));
        FileOutputFormat.setOutputPath(job_conf, new Path("output"));

        try {
            runJob(job_conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static RunningJob runJob(JobConf job) throws Exception {
        JobClient jc = new JobClient(job);
        RunningJob rj = jc.submitJob(job);
        if (!jc.monitorAndPrintJob(job, rj)) {
            throw new IOException("Job failed with info: " + rj.getFailureInfo());
        }
        return rj;
    }
}
