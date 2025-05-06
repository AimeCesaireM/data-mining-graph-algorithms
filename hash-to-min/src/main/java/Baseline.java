import java.io.*;
import java.util.*;

public class Baseline {

    /**
     * Given an undirected graph represented as an adjacency list,
     * returns a list of connected components, each represented as a Set of node IDs.
     */
    public static List<Set<String>> findComponents(Map<String, List<String>> graph) {
        List<Set<String>> components = new ArrayList<>();
        Set<String> visited = new HashSet<>();

        for (String node : graph.keySet()) {
            if (visited.contains(node)) continue;

            Set<String> component = new HashSet<>();
            Deque<String> stack = new ArrayDeque<>();
            stack.push(node);
            visited.add(node);

            while (!stack.isEmpty()) {
                String u = stack.pop();
                component.add(u);
                for (String v : graph.getOrDefault(u, Collections.emptyList())) {
                    if (!visited.contains(v)) {
                        visited.add(v);
                        stack.push(v);
                    }
                }
            }
            components.add(component);
        }
        return components;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: java Baseline <edge-list-file>");
            System.exit(1);
        }

        String filename = args[0];
        Map<String, List<String>> graph = new HashMap<>();

        // 1) Read edges from file
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.trim().split("\\t");
                if (parts.length < 2) continue;  // skip bad lines

                String u = parts[0];
                String v = parts[1];

                // add v to u's adjacency
                graph.computeIfAbsent(u, k -> new ArrayList<>()).add(v);
                // add u to v's adjacency (undirected)
                graph.computeIfAbsent(v, k -> new ArrayList<>()).add(u);
            }
        }

        // 2) Compute connected components
        double start = System.currentTimeMillis();
        List<Set<String>> comps = findComponents(graph);
        double end = System.currentTimeMillis();
        double elapsed = (end - start) / 1000.0;

        // 3) Print out each component in sorted order
        int idx = 1;
        for (Set<String> comp : comps) {
            List<String> sorted = new ArrayList<>(comp);
            Collections.sort(sorted);
            System.out.printf("Component %d: %s%n", idx++, sorted);
        }
        System.out.printf("Found %d connected components in %f seconds.%n", comps.size(), elapsed);
    }
}
