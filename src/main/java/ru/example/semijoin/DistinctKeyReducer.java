package ru.example.semijoin;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reducer, job1 Extract Distinct Customer IDs from Orders
public class DistinctKeyReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outputValue = new Text("ACTIVE");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // Since we only care about existence, emit each customer_id once
        // This gives us distinct customer_ids from orders
        context.write(key, outputValue);
        context.getCounter("JOB1_STATS", "DISTINCT_ACTIVE_CUSTOMERS").increment(1);
    }
}