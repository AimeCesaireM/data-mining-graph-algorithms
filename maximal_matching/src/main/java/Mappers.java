import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Mappers {
    public static class FirstRoundMapper extends Mapper<Object, Text, Text, IntWritable> {
        /*
         * First-round mapper
         */

        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Split input chunk (or transaction if block_size = 1), into transactions, by the newline delimiter
//            String[] transactionsArray = value.toString().trim().split("\\n");
//            List<String> transactions = Arrays.asList(transactionsArray);
            String line = value.toString();
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                return;
            }
            String[] parts = line.split("\\s+");
            if (parts.length < 2) return;

            int u = Integer.parseInt(parts[0]);
            int v = Integer.parseInt(parts[1]);
            if (u == v) return; // Ignore self-loops for simple MM

//            // Convert each transaction into a list of strings (itemsets)
//            List<List<String>> chunk = new ArrayList<>();
//            for (String transaction : transactions) {
//                String[] itemsArray = transaction.trim().split("\\s+");
//                List<String> items = Arrays.asList(itemsArray);
//                chunk.add(items);
//            }
//
//            // Compute the local min support
//            Configuration conf = context.getConfiguration();
//            double min_freq = conf.getDouble("min_freq", 0.50);
//            int local_support = (int) Math.ceil(min_freq * transactions_per_block);
//
//            // Invoking APriori with transactions (n = transactions_per_block)
//            APriori ap = new APriori(chunk);
//
//            // Calling instance method to find frequent itemsets with computed local support
//            Set<List<String>> localFrequentItemsets = ap.find_itemsets(
//                    new IntWritable(local_support));
//
//            // Process each returned candidate itemset before emission
//            for (List<String> itemset : localFrequentItemsets) {
//                // Sort itemsets to avoid duplicates
//                Collections.sort(itemset);
//                Text outputKey = new Text(String.join(" ", itemset));
//                // Emit local frequent itemset candidates with value = 1
            List<String> outKey = new ArrayList();
            outKey.add(String.valueOf(u));
            outKey.add(String.valueOf(v));
            Text p = new Text(String.join(", ", outKey));
            context.write(p, one);
//            }
        }

//        @Override
//        public void setup(Context context) {
//            Configuration conf = context.getConfiguration();
//            dataset_size = conf.getInt("dataset_size", 1);
//            transactions_per_block = conf.getInt("transactions_per_block", 1);
//        }
    }
    public static class SecondRoundMapper extends Mapper<Object, Text, Text, IntWritable> {
        /*
         * First-round mapper
         */

        int dssize;
        int transactions_per_block;
        double min_freq;

        List<Set<String>> candidatesSet = new ArrayList<>();
        Set<String> transactionItems = new HashSet<>();
        Text candidateKey = new Text();

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Process loaded value - one or multiple transactions of the original dataset (depending on SONMR variant)
            String blockOfTransactions = value.toString();

            // Split given block into individual transaction lines based on newline character
            String[] transactionLines = blockOfTransactions.split("\\n");

            // Exit if no intermediate values (emitted values) from first-round mapper
            if (candidatesSet.isEmpty()) {
                return;
            }

            // Iterate through each transaction line within the loaded block of original dataset
            for (String line : transactionLines) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                // Load transactions items to a set structure for faster O(1) lookup
                transactionItems.clear();
                transactionItems.addAll(Arrays.asList(line.split("\\s+")));

                // Check if current transaction contains any of the candidate itemsets
                for (Set<String> candidate : candidatesSet) {
                    // Check if the transaction contains all items in the candidate itemset
                    if (!candidate.isEmpty() && transactionItems.containsAll(candidate)) {
                        // Format each itemset using a space delimiter
                        String candidateStr = String.join(" ", candidate);

                        // Emit key-value pairs for final reducer aggregration
                        candidateKey.set(candidateStr);
                        context.write(candidateKey, one);
                    }
                }
            }
        }

        public void setup(Context context) {
            // Load the global command-line arguments
            Configuration conf = context.getConfiguration();
            dssize = conf.getInt("dssize", 1000);
            transactions_per_block = conf.getInt("transactions_per_block", 1);
            min_freq = conf.getDouble("min_freq", 0.5);
            try {
                // Load intermediate path from the cache
                String interm_path = context.getCacheFiles()[0].toString();

                // Extract the first-round emitted values (local candidate itemsets) from the intermediate file
                try (BufferedReader br = new BufferedReader(new FileReader(interm_path))) {

                    // Process each local candidate itemset
                    for (String line; (line = br.readLine()) != null; ) {
                        String[] items = line.trim().split("\\s+");
                        // Extract the candidate itemset minus the emitted void value (0)
                        String[] itemset_only = Arrays.copyOf(items, items.length - 1);
                        Set<String> candidateItemset = new HashSet<>(Arrays.asList(itemset_only));

                        // Add candidate itemset to set of all local frequent itemsets
                        candidatesSet.add(candidateItemset);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}