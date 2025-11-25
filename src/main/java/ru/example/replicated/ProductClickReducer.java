package ru.example.replicated;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.util.List;

public class ProductClickReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String category = key.toString();
        int viewCount = 0;
        int clickCount = 0;
        int purchaseCount = 0;
        List<String> sampleRecords = new ArrayList<>();

        for (Text value : values) {
            String record = value.toString();
            String[] fields = record.split(",");

            if (fields.length >= 4) {
                String action = fields[2];

                // Count different actions
                switch (action) {
                    case "view":
                        viewCount++;
                        break;
                    case "click":
                        clickCount++;
                        break;
                    case "purchase":
                        purchaseCount++;
                        break;
                    case "add_to_cart":
                        // Handle other actions if needed
                        break;
                }

                // Keep some sample records for output
                if (sampleRecords.size() < 5) {
                    sampleRecords.add(record);
                }
            }
        }

        // Calculate metrics
        double conversionRate = (purchaseCount > 0) ?
                (double) purchaseCount / viewCount * 100 : 0;

        // Prepare output
        String metrics = String.format(
                "Views: %d, Clicks: %d, Purchases: %d, Conversion: %.2f%%",
                viewCount, clickCount, purchaseCount, conversionRate
        );

        // Emit category analysis
        context.write(new Text(category), new Text(metrics));

        // Emit sample records for debugging
        for (String sample : sampleRecords) {
            context.write(new Text("SAMPLE_" + category), new Text(sample));
        }
    }
}