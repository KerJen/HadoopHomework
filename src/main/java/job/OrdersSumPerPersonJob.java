package job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import reduce.OrdersSumPerPersonReducer;
import map.OrdersSumPerPersonMapper;

public class OrdersSumPerPersonJob extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        JobConf job_conf = new JobConf(getClass());

        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(IntWritable.class);

        job_conf.setMapperClass(OrdersSumPerPersonMapper.class);
        job_conf.setReducerClass(OrdersSumPerPersonReducer.class);

        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf, new Path("input"));
        FileOutputFormat.setOutputPath(job_conf, new Path("output/orders_sum_per_person"));

        Job job = Job.getInstance(job_conf, "OrdersSumPerPerson");

        return job.waitForCompletion(true) ? 0 :1;
    }
}