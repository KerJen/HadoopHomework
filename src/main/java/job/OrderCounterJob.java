package job;

import map.OrdersCountMapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import reduce.OrdersCountReducer;

public class OrderCounterJob extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        JobConf job_conf = new JobConf(getClass());

        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(IntWritable.class);

        job_conf.setMapperClass(OrdersCountMapper.class);
        job_conf.setReducerClass(OrdersCountReducer.class);

        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf, new Path("input"));
        FileOutputFormat.setOutputPath(job_conf, new Path("output/orders_count"));

        Job job = Job.getInstance(job_conf, "Orders");

        return job.waitForCompletion(true) ? 0 :1;
    }
}
