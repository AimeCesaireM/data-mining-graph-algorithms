import java.io.*;
import java.util.*;


public class MMSequential {

    // Adjacency list representation of the input graph
    private Map<Integer, Set<Integer>> adj;

    public MMSequential(Map<Integer, Set<Integer>> graph) {
        this.adj = graph;
    }

    // --- Standard Greedy Maximal Matching (MM) Implementation ---
    // Iterate through nodes and greedily add edges if both endpoints are unmatched.
    public Map<Integer, Integer> findGreedyMaximalMatching() {
        Map<Integer, Integer> matching = new HashMap<>(); // Stores matched pairs (u -> v and v -> u alike)
        Set<Integer> matchedNodes = new HashSet<>(); // Tracks nodes already in the matching

        System.out.println("Starting Standard Greedy Maximal Matching...");
        long startTime = System.nanoTime();

        // Iterate through all nodes as potential starting points for an edge
        List<Integer> nodes = new ArrayList<>(adj.keySet());
        Collections.shuffle(nodes, new Random()); // Add randomness to potentially get different maximal matchings

        for (int u : nodes) {
            if (!matchedNodes.contains(u)) { // If u is not already matched
                // Look for an available (or unmatched) neighbor v
                Set<Integer> neighbors = adj.getOrDefault(u, Collections.emptySet());
                for (int v : neighbors) {
                    if (!matchedNodes.contains(v)) { // If v is also not matched
                        // Add (u, v) to the matching
                        matching.put(u, v);
                        matching.put(v, u);
                        // Mark both nodes as matched
                        matchedNodes.add(u);
                        matchedNodes.add(v);
                        // Break inner loop: u is now matched, move to the next node in outer loop
                        break;
                    }
                }
            }
        }

        long endTime = System.nanoTime();
        System.out.printf("Standard Greedy MM finished in %.3f ms%n", (endTime - startTime) / 1_000_000.0);
        // The size of the matching is half the size of the map (the number of nodes)
        System.out.println("Standard Greedy MM Size: " + matching.size() / 2);
        return matching;
    }

    // --- Graph Loading Utility ---
    public static Map<Integer, Set<Integer>> loadGraph(String filePath) throws IOException {
        Map<Integer, Set<Integer>> adj = new HashMap<>();
        System.out.println("Attempting to load graph from: " + filePath);
        long edgesRead = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] parts = line.split("\\s+");
                if (parts.length < 2) continue;

                try {
                    int u = Integer.parseInt(parts[0]);
                    int v = Integer.parseInt(parts[1]);
                    if (u == v) continue; // Ignore self-loops

                    adj.computeIfAbsent(u, k -> new HashSet<>()).add(v);
                    adj.computeIfAbsent(v, k -> new HashSet<>()).add(u);
                    edgesRead++;
                    if (edgesRead % 5000 == 0) { // To track progress for smaller graphs
                        System.out.println("Read " + edgesRead + " edges...");
                    }

                } catch (NumberFormatException e) {
                    // Ignore lines with non-integer values
                    System.err.println("Formatting error while loading graph.");
                    System.err.println("Error: " + e.getMessage());
                }
            }
        }
        System.out.println("Finished loading graph. Nodes: " + adj.size() + ", Edges processed: " + edgesRead);
        // Calculate actual unique edges
        long degreeSum = 0;
        for (Set<Integer> neighbors : adj.values()) {
            degreeSum += neighbors.size();
        }
        System.out.println("Unique edges in graph: " + degreeSum / 2);
        return adj;
    }

    // --- Graph Saving Utility ---
    public static void saveGraph(Map<Integer, Integer> graphToSave, String filePath) throws IOException {
        System.out.println("Attempting to save graph to: " + filePath);
        long edgesWritten = 0;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (Map.Entry<Integer, Integer> entry : graphToSave.entrySet()) {
                int source = entry.getKey();
                int target = entry.getValue();
                String edge = String.format("%-10d\t%d", source, target);
                writer.write(edge);
                writer.newLine();
                edgesWritten++;
                if (edgesWritten % 2000 == 0) { // To track progress for smaller graphs
                    System.out.println("Written " + edgesWritten + " edges...");
                }
            }
        }
        System.out.println("Finished saving directed graph. Total edges written: " + edgesWritten);
    }

    // --- Main Method ---
    public static void main(String[] args) {
        String filePath = "datasets/ca-GrQc.txt"; // path to graph
//        String outPath = "graphs/max_matching"; // path to output file
        Map<Integer, Integer> maxMatching = new HashMap<>();

        try {
            System.out.println("Loading graph...");
            Map<Integer, Set<Integer>> graph = loadGraph(filePath);

            if (graph.isEmpty()) {
                System.err.println("Graph could not be loaded or is empty.");
                return;
            }
            System.out.println("Graph loaded successfully.");

            // --- Run Standard Greedy MM ---
            MMSequential mmInstance = new MMSequential(graph); // Pass the loaded graph
            maxMatching = mmInstance.findGreedyMaximalMatching();
//            saveGraph(maxMatching, outPath);
            System.out.println("----------------------------------------");

        } catch (IOException e) {
            System.err.println("Failed to load or save graph from a given path.");
            System.err.println("Error: " + e.getMessage());
            System.err.println("Please ensure the file exists and the path is correct.");
        } catch (OutOfMemoryError e) {
            System.err.println("OutOfMemoryError: The graph might be too large for the available heap space.");
            System.err.println("Try increasing the heap size for your Java runtime (e.g., -Xmx4g or more in IntelliJ run configuration).");
        } catch (Exception e) {
            System.err.println("An unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}