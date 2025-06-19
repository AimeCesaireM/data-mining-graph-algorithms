import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Set;

// This class implements a sequential greedy maximal matching algorithm used in sampling and post-filtering phases
public class SequentialMaximalMatching {

    // Finds a greedy maximal matching from an InputStream and writes it to an OutputStream.
    public void findMatching(InputStream inputStream, OutputStream outputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        Set<String> matchedVertices = new HashSet<>();

        String line;
        while ((line = reader.readLine()) != null) {
            String[] vertices = line.trim().split(",");
            if (vertices.length == 2) {
                String u = vertices[0].trim();
                String v = vertices[1].trim();

                // Check if none of the endponts is already in the matching
                if (!matchedVertices.contains(u) && !matchedVertices.contains(v)) {
                    writer.write(u + "," + v);
                    writer.newLine();
                    matchedVertices.add(u);
                    matchedVertices.add(v);
                }
            }
        }
    }
}