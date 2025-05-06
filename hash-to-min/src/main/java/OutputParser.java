import java.io.*;
import java.util.*;

public class OutputParser {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: java OutputParser <input-file> <output-file>");
            System.exit(1);
        }
        String inputPath  = args[0];
        String outputPath = args[1];

        // 1) Read & canonicalize clusters, collapse duplicates
        // Map: canonicalClusterString -> representativeKey
        Map<String, String> canonToRep = new HashMap<>();
        // Map: canonicalClusterString -> clusterSet
        Map<String, Set<String>> canonToSet = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(inputPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || !line.contains("\t")) continue;

                String[] parts = line.split("\t", 2);
                String key = parts[0];
                String[] vals = parts[1].split(",");

                // Build the full set S = {key} ∪ vals
                Set<String> cluster = new HashSet<>();
                cluster.add(key);
                for (String v : vals) {
                    if (!v.isEmpty()) cluster.add(v);
                }

                // Canonical string: sorted, comma-joined
                List<String> sorted = new ArrayList<>(cluster);
                Collections.sort(sorted);
                String canon = String.join(",", sorted);

                // Collapse duplicates: pick lexicographically smallest representative
                if (canonToRep.containsKey(canon)) {
                    String oldRep = canonToRep.get(canon);
                    if (key.compareTo(oldRep) < 0) {
                        canonToRep.put(canon, key);
                    }
                } else {
                    canonToRep.put(canon, key);
                    canonToSet.put(canon, cluster);
                }
            }
        }

        // 2) Extract unique clusters and sort by size descending
        List<Set<String>> allSets = new ArrayList<>(canonToSet.values());
        allSets.sort((a, b) -> Integer.compare(b.size(), a.size()));

        // 3) Keep only maximal clusters (drop any strict subset)
        List<Set<String>> maximal = new ArrayList<>();
        for (Set<String> s : allSets) {
            boolean isSubset = false;
            for (Set<String> m : maximal) {
                if (m.containsAll(s)) {
                    isSubset = true;
                    break;
                }
            }
            if (!isSubset) maximal.add(s);
        }

        // 4) Write results
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath))) {
            for (Set<String> s : maximal) {
                // Build canonical string again to look up rep
                List<String> sorted = new ArrayList<>(s);
                Collections.sort(sorted);
                String canon = String.join(",", sorted);

                String rep = canonToRep.get(canon);

                // Members = all elements except the rep
                List<String> members = new ArrayList<>(sorted);
                members.remove(rep);
                String memberStr = String.join(",", members);

                bw.write(rep + "\t" + memberStr);
                bw.newLine();
            }
        }

        System.out.printf("Wrote %,d unique maximal clusters to %s%n", maximal.size(), outputPath);
    }
}
