package job;

import map.DayFrequencyMapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import reduce.DayFrequencyReducer;
import reduce.OrdersCountReducer;

public class DayFrequencyJob extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        JobConf job_conf = new JobConf(getClass());

        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(IntWritable.class);

        job_conf.setMapperClass(DayFrequencyMapper.class);
        job_conf.setReducerClass(DayFrequencyReducer.class);

        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf, new Path("input"));
        FileOutputFormat.setOutputPath(job_conf, new Path("output/day_frequency"));

        Job job = Job.getInstance(job_conf, "DayFrequency");

        return job.waitForCompletion(true) ? 0 :1;
    }
}
