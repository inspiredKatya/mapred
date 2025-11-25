package ru.example.replicated;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.filecache.DistributedCache;

public class ProductClickMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Map<String, String> productInfo = new HashMap<>();

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        // Load the small products file from Distributed Cache
        Path[] cacheFiles =  DistributedCache.getLocalCacheFiles(context.getConfiguration());
        if (cacheFiles != null && cacheFiles.length > 0) {
            loadProductData(cacheFiles[0], context);
        }
    }

    private void loadProductData(Path filePath, Context context) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath.toString()));
        String line;

        // Skip header if exists
        reader.readLine();

        while ((line = reader.readLine()) != null) {
            String[] fields = line.split(",");
            if (fields.length >= 4) {
                String productId = fields[0].trim();
                String productName = fields[1].trim();
                String category = fields[2].trim();
                String price = fields[3].trim();

                // Store product info in memory
                String productDetails = productName + "|" + category + "|" + price;
                productInfo.put(productId, productDetails);

                context.getCounter("PRODUCT_STATS", "PRODUCTS_LOADED").increment(1);
            }
        }
        reader.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header row in clicks data
        if (line.startsWith("user_id")) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length >= 4) {
            String userId = fields[0].trim();
            String timestamp = fields[1].trim();
            String productId = fields[2].trim();
            String action = fields[3].trim();

            // Perform the join in-memory
            if (productInfo.containsKey(productId)) {
                String productDetails = productInfo.get(productId);

                // Create enriched output
                String outputValue = String.join(",",
                        userId,
                        timestamp,
                        action,
                        productDetails
                );

                // Emit with category as key for further analysis
                String[] details = productDetails.split("\\|");
                String category = details[1];

                context.write(new Text(category), new Text(outputValue));

                context.getCounter("CLICK_STATS", "JOINED_RECORDS").increment(1);
            } else {
                context.getCounter("CLICK_STATS", "UNKNOWN_PRODUCT").increment(1);
            }
        }
    }
}