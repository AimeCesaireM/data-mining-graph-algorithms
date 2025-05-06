import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SingleJobDriver extends Configured {
    private final Configuration conf;

    public SingleJobDriver(Configuration conf) {
        this.conf = conf;
    }

    public Job run(Path inputPath, Path outputPath, int iteration) throws Exception {

        Job job = Job.getInstance(conf, "hash-to-min-" + iteration);
        job.setJarByClass(SingleJobDriver.class);

        // 1) Mapper & Reducer
        job.setMapperClass(HashToMinMapper.class);
        job.setReducerClass(HashToMinReducer.class);

        // 2) Map output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ClusterWritable.class);

        // 3) Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        // 4) I/O paths
        FileInputFormat.addInputPath (job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);


        // will think about reducers later

        job.submit();
        return job;
    }
}
