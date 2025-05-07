import java.io.*;
import java.util.*;

// Implements a standard sequential greedy maximal matching algorithm to use as a baseline
public class BaselineGreedyAlgo {

    private Map<Integer, Set<Integer>> adj; // Adjacency list to represent the graph.

    // Constructor to initialize the graph.
    public BaselineGreedyAlgo(Map<Integer, Set<Integer>> graph) {
        this.adj = graph;
    }

    // Finds a greedy maximal matching in the graph.
    public Map<Integer, Integer> findGreedyMaximalMatching() {
        Map<Integer, Integer> matching = new HashMap<>();
        Set<Integer> matchedNodes = new HashSet<>();

        List<Integer> nodes = new ArrayList<>(adj.keySet());
        Collections.shuffle(nodes, new Random());

        for (int u : nodes) {
            if (!matchedNodes.contains(u)) {
                // Get neighbors of the current node.
                Set<Integer> neighbors = adj.getOrDefault(u, Collections.emptySet());
                // Look for an unmatched neighbor.
                for (int v : neighbors) {
                    // If the neighbor is also not matched.
                    if (!matchedNodes.contains(v)) {
                        matching.put(u, v);
                        // mark both u and v as matched
                        matchedNodes.add(u);
                        matchedNodes.add(v);
                        break; // u is now matched, move to the next node.
                    }
                }
            }
        }
        return matching; // Return the computed maximal matching.
    }

    // Utility to load a graph from a file.
    public static Map<Integer, Set<Integer>> loadGraph(String filePath) throws IOException {
        Map<Integer, Set<Integer>> adj = new HashMap<>();
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
                    if (u == v) continue;

                    adj.computeIfAbsent(u, k -> new HashSet<>()).add(v);
                    adj.computeIfAbsent(v, k -> new HashSet<>()).add(u);
                } catch (NumberFormatException e) {
                    System.err.println("Formatt error while loading the graph: " + e.getMessage());
                }
            }
        }
        return adj; // Return the loaded graph adjacency list.
    }

    // Utility to save a matching to a file.
    public static void saveGraph(Map<Integer, Integer> graphToSave, String filePath) throws IOException {
        // Write matching edges to file.
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (Map.Entry<Integer, Integer> entry : graphToSave.entrySet()) {
                int source = entry.getKey();
                int target = entry.getValue();
                String edge = String.format("%-10d\t%d", source, target);
                writer.write(edge);
                writer.newLine();
            }
        }
    }

    // Main method to run the greedy algorithm.
    public static void main(String[] args) {
        String filePath = "src/input/web-Google.txt";
        String outPath = "src/graphs/max_matching";
        Map<Integer, Integer> maxMatching;

        try {
            Map<Integer, Set<Integer>> graph = loadGraph(filePath);

            if (graph.isEmpty()) {
                System.err.println("The graph could not be loaded or is empty.");
                return;
            }

            BaselineGreedyAlgo mmInstance = new BaselineGreedyAlgo(graph);
            maxMatching = mmInstance.findGreedyMaximalMatching();
            saveGraph(maxMatching, outPath);

        } catch (IOException e) {
            System.err.println("Failed to load or save graph: " + e.getMessage());
        } catch (OutOfMemoryError e) {
            System.err.println("OutOfMemoryError: The graph might be too large.");
        } catch (Exception e) {
            System.err.println("An unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}