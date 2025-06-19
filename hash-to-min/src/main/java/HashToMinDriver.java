// HashToMinDriver.java
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.*;

public class HashToMinDriver extends Configured implements Tool {
    private static final int MAX_ITERS = 20;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HashToMinDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: HashToMinDriver <inputDir> <outputBaseDir>");
            return -1;
        }

        Configuration conf = getConf();
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        FileSystem fs = FileSystem.get(conf);
        SingleJobDriver singleRun = new SingleJobDriver(conf);

        Path prev = new Path(args[0]);
        Path next = new Path(args[1] + "/iter0");

        boolean converged = false;
        int iter = 0;
        double time = 0;

        while (!converged && iter < MAX_ITERS) {
            double start = System.currentTimeMillis();
            // 1) launch one iteration
            Job job = singleRun.run(prev, next, iter);

            // 2) wait for completion
            if (!job.waitForCompletion(true)) {
                System.err.println("Iteration " + iter + " failed.");
                return 1;
            }

            // 3) after the first iteration, check if output == previous
            if (iter > 0) {
                if (dirsAreEqual(fs, prev, next)) {
                    converged = true;
                    // clean up the now-unneeded prev dir
                    fs.delete(prev, true);
                    break;
                } else {
                    fs.delete(prev, true);
                }
            }
            double end = System.currentTimeMillis();
            double elapsed = (end - start) / 1000.0;
            time += elapsed;

            // 4) prepare for next round
            prev = next;
            iter++;
            next = new Path(args[1] + "/iter" + iter);
        }
        double avgTime = time / iter;
        System.out.println("Converged in " + iter + " iteration(s).");
        System.out.printf("Average time per iteration: %.2f seconds%n", avgTime);
        return 0;
    }

    // Compare two HDFS directories: true iff every file line-for-line matches
    private boolean dirsAreEqual(FileSystem fs, Path dir1, Path dir2) throws IOException {
        List<Path> list1 = listSortedFiles(fs, dir1);
        List<Path> list2 = listSortedFiles(fs, dir2);
        if (list1.size() != list2.size()) return false;
        for (int i = 0; i < list1.size(); i++) {
            if (!fileEquals(fs, list1.get(i), list2.get(i))) return false;
        }
        return true;
    }

    private List<Path> listSortedFiles(FileSystem fs, Path dir) throws IOException {
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(dir, false);
        List<Path> paths = new ArrayList<>();
        while (it.hasNext()) paths.add(it.next().getPath());
        Collections.sort(paths);
        return paths;
    }

    private boolean fileEquals(FileSystem fs, Path f1, Path f2) throws IOException {
        try (BufferedReader r1 = new BufferedReader(new InputStreamReader(fs.open(f1)));
             BufferedReader r2 = new BufferedReader(new InputStreamReader(fs.open(f2)))) {
            String l1, l2;
            while (true) {
                l1 = r1.readLine();
                l2 = r2.readLine();
                if (l1 == null && l2 == null) return true;
                if (l1 == null || l2 == null || !l1.equals(l2)) return false;
            }
        }
    }
}
