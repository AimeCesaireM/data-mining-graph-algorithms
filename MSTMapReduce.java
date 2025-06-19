import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Multi-round MapReduce MST with filtering-based recursion.
 * Records total runtime and number of rounds taken.
 */
public class MSTMapReduce {
    public static final String EDGE_COUNTER_GROUP = "MST";
    public static final String EDGE_COUNTER = "EDGES";

    public static class EdgeMapper extends Mapper<Object, Text, IntWritable, Text> {
        private int numBuckets;
        private IntWritable bucketId = new IntWritable();
        private Text edgeText = new Text();

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            numBuckets = conf.getInt("mst.partitions", 10);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            edgeText.set(line);
            int bid = Math.floorMod(line.hashCode(), numBuckets);
            bucketId.set(bid);
            context.write(bucketId, edgeText);
        }
    }

    public static class LocalMSTReducer extends Reducer<IntWritable, Text, Text, Text> {
        private Text uOut = new Text(), vWOut = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Edge> edges = new ArrayList<>();
            Set<Integer> verts = new HashSet<>();
            for (Text t : values) {
                String[] p = t.toString().split("\\s+");
                int u = Integer.parseInt(p[0]), v = Integer.parseInt(p[1]);
                double w = Double.parseDouble(p[2]);
                edges.add(new Edge(u, v, w));
                verts.add(u); verts.add(v);
            }
            List<Edge> mst = kruskalMST(edges, verts);
            for (Edge e : mst) {
                uOut.set(String.valueOf(e.u));
                vWOut.set(e.v + " " + e.w);
                context.write(uOut, vWOut);
                context.getCounter(EDGE_COUNTER_GROUP, EDGE_COUNTER).increment(1);
            }
        }
    }

    public static class GlobalMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final static IntWritable oneKey = new IntWritable(0);
        private Text edgeText = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            edgeText.set(value.toString());
            context.write(oneKey, edgeText);
        }
    }

    public static class GlobalReducer extends Reducer<IntWritable, Text, Text, Text> {
        private Text uOut = new Text(), vWOut = new Text();
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Edge> edges = new ArrayList<>();
            Set<Integer> verts = new HashSet<>();
            for (Text t : values) {
                String[] p = t.toString().split("\\s+");
                int u = Integer.parseInt(p[0]), v = Integer.parseInt(p[1]);
                double w = Double.parseDouble(p[2]);
                edges.add(new Edge(u, v, w));
                verts.add(u); verts.add(v);
            }
            List<Edge> mst = kruskalMST(edges, verts);
            for (Edge e : mst) {
                uOut.set(String.valueOf(e.u));
                vWOut.set(e.v + " " + e.w);
                context.write(uOut, vWOut);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: MSTMapReduce <input> <outputDir> <maxRounds> [partitions] [threshold]");
            System.exit(2);
        }
        String input = args[0];
        String baseOutput = args[1];
        int maxRounds = Integer.parseInt(args[2]);
        int partitions = args.length > 3 ? Integer.parseInt(args[3]) : 10;
        long threshold = args.length > 4 ? Long.parseLong(args[4]) : 1000000L;

        long programStart = System.currentTimeMillis();
        String currentInput = input;
        int roundsUsed = 0;

        for (int round = 1; round <= maxRounds; round++) {
            roundsUsed = round;
            String tmpOut = baseOutput + "/round" + round;
            Configuration conf = new Configuration();
            conf.setInt("mst.partitions", partitions);

            Job job = Job.getInstance(conf, "Local MST Round " + round);
            job.setJarByClass(MSTMapReduce.class);
            job.setMapperClass(EdgeMapper.class);
            job.setReducerClass(LocalMSTReducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(partitions);
            FileInputFormat.addInputPath(job, new Path(currentInput));
            FileOutputFormat.setOutputPath(job, new Path(tmpOut));

            long roundStart = System.currentTimeMillis();
            job.waitForCompletion(true);
            long roundDur = System.currentTimeMillis() - roundStart;
            System.out.println("[INFO] Round " + round + " duration (ms): " + roundDur);

            Counters ctrs = job.getCounters();
            long edgeCount = ctrs.findCounter(EDGE_COUNTER_GROUP, EDGE_COUNTER).getValue();
            System.out.println("Round " + round + ": edges=" + edgeCount);
            if (edgeCount <= threshold) {
                // final global merge
                Path finalIn = new Path(tmpOut);
                Path finalOut = new Path(baseOutput + "/final");
                Job merge = Job.getInstance(new Configuration(), "Global MST Merge");
                merge.setJarByClass(MSTMapReduce.class);
                merge.setMapperClass(GlobalMapper.class);
                merge.setReducerClass(GlobalReducer.class);
                merge.setMapOutputKeyClass(IntWritable.class);
                merge.setMapOutputValueClass(Text.class);
                merge.setOutputKeyClass(Text.class);
                merge.setOutputValueClass(Text.class);
                merge.setNumReduceTasks(1);
                FileInputFormat.addInputPath(merge, finalIn);
                FileOutputFormat.setOutputPath(merge, finalOut);

                long mergeStart = System.currentTimeMillis();
                merge.waitForCompletion(true);
                long mergeDur = System.currentTimeMillis() - mergeStart;
                System.out.println("[INFO] Global merge duration (ms): " + mergeDur);

                long totalDur = System.currentTimeMillis() - programStart;
                System.out.println("Completed in " + roundsUsed + " rounds. Total runtime (ms): " + totalDur);
                System.exit(0);
            }
            currentInput = tmpOut;
        }
        long totalDur = System.currentTimeMillis() - programStart;
        System.err.println("Reached maxRounds (" + roundsUsed + ") without reducing below threshold. Total runtime (ms): " + totalDur);
        System.exit(1);
    }

    // Kruskal + Union-Find
    static class Edge implements Comparable<Edge> {
        int u, v; double w;
        Edge(int u, int v, double w) { this.u = u; this.v = v; this.w = w; }
        public int compareTo(Edge o) { return Double.compare(this.w, o.w); }
    }

    static List<Edge> kruskalMST(List<Edge> edges, Set<Integer> verts) {
        Collections.sort(edges);
        UnionFind uf = new UnionFind(verts);
        List<Edge> mst = new ArrayList<>();
        for (Edge e : edges) if (uf.union(e.u, e.v)) mst.add(e);
        return mst;
    }

    static class UnionFind {
        private Map<Integer,Integer> p;
        UnionFind(Set<Integer> vs) { p = new HashMap<>(); for (int v: vs) p.put(v,v); }
        int find(int x) { int parent = p.get(x); if (parent!=x) { parent = find(parent); p.put(x,parent);} return parent; }
        boolean union(int a,int b){ int ra=find(a), rb=find(b); if(ra==rb) return false; p.put(ra,rb); return true; }
    }
}
