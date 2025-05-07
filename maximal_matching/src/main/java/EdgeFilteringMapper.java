import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;

// Mapper for filterng edges that have endpoints which have been matched in previous runds.
public class EdgeFilteringMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Set<String> matchedVertices = new HashSet<>(); // Set to store vertices already mached.
    private Text edgeOutput = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        // Extract the mached vertices from the cached file
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            for (URI cacheFile : cacheFiles) {
                Path matchedVerticesPath = new Path(cacheFile.getPath());
                FileSystem fs = FileSystem.get(context.getConfiguration());
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(matchedVerticesPath)))) {
                    String vertex;
                    while ((vertex = reader.readLine()) != null) {
                        matchedVertices.add(vertex.trim());
                    }
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] vertices = line.split("\\s+|,");

        if (vertices.length == 2) {
            String u = vertices[0];
            String v = vertices[1];

            // Check if neither endpont in the set of matched vertices.
            if (!matchedVertices.contains(u) && !matchedVertices.contains(v)) {
                String edgeString;
                // A format for edge representaton
                if (u.compareTo(v) < 0) {
                    edgeString = u + "," + v;
                } else {
                    edgeString = v + "," + u;
                }
                edgeOutput.set(edgeString);
                // Emit only edges not filterd out
                context.write(edgeOutput, NullWritable.get());
            }
        }
    }
}