import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.getConf;

public class Driver {

    public static void main(String[] args) throws Exception {
        int iteration = 0;
        boolean isDone = false;
        // create jobs until we finish
        while (!isDone && iteration < 20) {
            Job job = Job.getInstance(getConf(), "HashToMin Iteration " + iteration);
            job.setJarByClass(Driver.class);

            job.setMapperClass(HashToMinMapper.class);
            job.setReducerClass(HashToMinReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(ClusterWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job, new Path(args[0] + "/iter" + iteration));
            FileOutputFormat.setOutputPath(job, new Path(args[0] + "/iter" + (iteration + 1)));

            isDone = job.waitForCompletion(true);
            iteration++;
        }
    }



}
