import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;

import java.io.*;
import java.net.URI;

// Class that manages the parallel maximal matching algorithm
public class MaximalMatchingDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path outputBasePath = new Path(args[1]);

        long totalInitialEdges = Long.parseLong(args[2]);
        conf.setLong("total_edges", totalInitialEdges);

        // Memory threshold as specified in (Lattanzi et al., 2011)
        long mem_thresh = Long.parseLong(args[3]);

        Path currentInputPath = inputPath;
        Path matchedVerticesPath = null;
        Path nextInputPath = null;
        long currentEdgeSetSize = totalInitialEdges;

        // Get the Hadoop filesystem for file/memory access
        FileSystem fs = FileSystem.get(conf);

        // Clean up output directory from previous runs to prevnt errors as an empty/non-existing directory/path is expected
        fs.delete(outputBasePath, true);

        // Directory to store the final maximal matching (and those from each sequential algorithm)
        Path finalMatchingPath = new Path(outputBasePath, "final_maximal_matching");
        fs.mkdirs(finalMatchingPath);

        int iter = 1;
        while (true){
            conf.setInt("current_iter", iter);
            conf.setLong("mem_thresh", mem_thresh);
            conf.setLong("current_edges_count", currentEdgeSetSize);

            // Round 1: Sampling edges
            Path sampledEdgesOutputPath = new Path(outputBasePath, "sampled_edges_round_" + iter);
            Job job1 = Job.getInstance(conf, "MM - Sampling " + iter);
            job1.setJarByClass(MaximalMatchingDriver.class);
            job1.setMapperClass(SamplingMapper.class);
            job1.setReducerClass(SamplingReducer.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(NullWritable.class);
            FileInputFormat.addInputPath(job1, currentInputPath);
            FileOutputFormat.setOutputPath(job1, sampledEdgesOutputPath);

            // Clean up output directory from previous runs to prevnt errors as an empty/non-existing directory/path is expected
            fs.delete(sampledEdgesOutputPath, true);

            if (!job1.waitForCompletion(true)) {
                System.err.println("Round 1 (Sampling) failed in iteration " + iter);
                System.exit(1);
            }

            // Sequential Maximal Matching on sampled edges.
            Path sequentialMatchingOutputPath = new Path(outputBasePath, "sequential_matching_round_" + iter);
            Path sequentialMatchingOutputPart = new Path(sequentialMatchingOutputPath, "part-r-00000");

            // Handle directory for sequential step
            fs.delete(sequentialMatchingOutputPath, true);
            fs.mkdirs(sequentialMatchingOutputPath);

            // Run sequential maximal matching on sampled edges
            try (
                    // Confirm input and output streams have been create
                    InputStream is = fs.open(findFirstPartFile(fs, sampledEdgesOutputPath));
                    OutputStream os = fs.create(sequentialMatchingOutputPart);
            ) {
                SequentialMaximalMatching sequentialMatcher = new SequentialMaximalMatching();
                sequentialMatcher.findMatching(is, os);
            } catch (IOException e) {
                System.err.println("Error from running the sequential matching for iteration " + iter + ": " + e.getMessage());
                System.exit(1);
            }

            // Round 2: Identifies the matched vertices from the sequential matching
            matchedVerticesPath = new Path(outputBasePath, "matched_vertices_round_" + iter);
            Job job2 = Job.getInstance(conf, "MM - Identify Matched Vertices " + iter);
            job2.setJarByClass(MaximalMatchingDriver.class);
            job2.setMapperClass(MatchedVertexMapper.class);
            job2.setReducerClass(MatchedVertexReducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(NullWritable.class);
            FileInputFormat.addInputPath(job2, sequentialMatchingOutputPath);
            FileOutputFormat.setOutputPath(job2, matchedVerticesPath);

            fs.delete(matchedVerticesPath, true);

            if (!job2.waitForCompletion(true)) {
                System.err.println("Round 2 (Matched Vertices) failed in iteration " + iter);
                System.exit(1);
            }

            // Round 3: Filtering edges for the next round.
            nextInputPath = new Path(outputBasePath, "filtered_edges_round_" + iter);
            Job job3 = Job.getInstance(conf, "MM - Edge Filtering " + iter);
            job3.setJarByClass(MaximalMatchingDriver.class);
            job3.setMapperClass(EdgeFilteringMapper.class);
            job3.setReducerClass(EdgeFilteringReducer.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(NullWritable.class);

            // Set input path for Job 3 (edges from previous round) and output for this round
            FileInputFormat.addInputPath(job3, currentInputPath);
            FileOutputFormat.setOutputPath(job3, nextInputPath);

            fs.delete(nextInputPath, true);

            // Add the matched vertices file to cache for the mapper
            Path matchedVerticesPartFile = new Path(matchedVerticesPath, "part-r-00000");
            if (!fs.exists(matchedVerticesPartFile)) {
                System.err.println("Matched vertices file not found for Distributed Cache in iteration " + iter);
                System.exit(1);
            }
            job3.addCacheFile(new URI(matchedVerticesPartFile.toUri().toString()));

            if (!job3.waitForCompletion(true)) {
                System.err.println("Round 3 (Edge Filtering) failed in iteration " + iter);
                System.exit(1);
            }

            // Get the number of remaining edges from Job 3's counter
            currentEdgeSetSize = job3.getCounters().findCounter("GraphStats", "RemainingEdges").getValue();

            // Add the matched edges from this iteration's sequential step to the final matching directory (for debuging)
            Path finalMatchingIterationPart = new Path(finalMatchingPath, "part-r-iter-" + iter);
            // Copy the sequential matching file into the final matching directory with a unique name.
            try {
                FileUtil.copy(fs, sequentialMatchingOutputPart, fs, finalMatchingIterationPart, false, conf);
            } catch (IOException e) {
                System.err.println("Error copying sequential matching part for iteration " + iter + ": " + e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }


            // Check if the remaining graph size is small enough to stop iterating.
            if (currentEdgeSetSize <= mem_thresh && currentEdgeSetSize > 0) {
                currentInputPath = nextInputPath;
                break;
            } else if (currentEdgeSetSize == 0) {
                currentInputPath = nextInputPath;
                break;
            } else {
                // Prepare for the next iteration: input is the output of round 3.
                currentInputPath = nextInputPath;
            }
            iter += 1; // Increment iteration counter.
        }


        // If edges remain and the size is below the memory threshold, run sequential matching on the rest.
        if (currentEdgeSetSize > 0) { // Only run if there are edges left

            Path finalSequentialMatchingOutputPath = new Path(outputBasePath, "final_sequential_matching");
            Path finalSequentialMatchingOutputPart = new Path(finalSequentialMatchingOutputPath, "part-r-00000");

            fs.delete(finalSequentialMatchingOutputPath, true);
            fs.mkdirs(finalSequentialMatchingOutputPath);

            InputStream finalSequentialInputStream;
            Path inputForFinalSequential = currentInputPath;

            try {
                if (iter > 1) {
                    // Input is the output of the last round
                    finalSequentialInputStream = fs.open(findFirstPartFile(fs, inputForFinalSequential));
                } else {
                    // Input is the original inputPath since iter==1
                    if (fs.isDirectory(inputForFinalSequential)) {
                        FileStatus[] inputFiles = fs.listStatus(inputForFinalSequential, path -> !path.getName().startsWith("_") && !path.getName().startsWith("."));
                        if (inputFiles.length > 0) {
                            finalSequentialInputStream = fs.open(inputFiles[0].getPath());
                        } else {
                            throw new IOException("Input directory is empty: " + inputForFinalSequential);
                        }
                    } else {
                        // Open direclty if its just a single file
                        finalSequentialInputStream = fs.open(inputForFinalSequential);
                    }
                }

                // Run sequential maximal matching on the remaining edges.
                try (
                        InputStream is = finalSequentialInputStream;
                        OutputStream os = fs.create(finalSequentialMatchingOutputPart);
                ) {
                    SequentialMaximalMatching sequentialMatcher = new SequentialMaximalMatching(); // Create sequential matcher.
                    sequentialMatcher.findMatching(is, os); // Run sequential algorithm.
                }

            } catch (IOException e) {
                System.err.println("Error running final sequential matching: " + e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }

            Path finalMatchingFinalPart = new Path(finalMatchingPath, "part-r-final-sequential");
            try {
                FileUtil.copy(fs, finalSequentialMatchingOutputPart, fs, finalMatchingFinalPart, false, conf);
            } catch (IOException e) {
                System.err.println("Error copying final sequential matching part: " + e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }
        }
        // Exit upon suceess
        System.exit(0);
    }

    // Finds the first "part-r-" file in a directory.
    private static Path findFirstPartFile(FileSystem fs, Path directoryPath) throws IOException {
        FileStatus[] statuses = null;
        statuses = fs.listStatus(directoryPath, path -> path.getName().startsWith("part-r-"));

        if (statuses != null && statuses.length > 0) {
            return statuses[0].getPath();
        }
        throw new IOException("No file found starting with part-r- in directory: " + directoryPath + ". Directory may be empty or job output failed.");
    }
}