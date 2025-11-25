package ru.example.semijoin;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CustomerFilterReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // For each active customer, we can perform additional analysis
        for (Text value : values) {
            String customerData = value.toString();

            // Example: Add analytics or transform data
            String enrichedData = "ACTIVE_CUSTOMER:" + customerData;

            context.write(key, new Text(enrichedData));
            context.getCounter("JOB2_STATS", "FINAL_OUTPUT_RECORDS").increment(1);
        }
    }
}