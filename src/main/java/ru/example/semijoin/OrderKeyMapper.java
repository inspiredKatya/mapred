package ru.example.semijoin;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper, job1 Extract Distinct Customer IDs from Orders
public class OrderKeyMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outputKey = new Text();
    private final Text outputValue = new Text("ORDER");

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header
        if (line.startsWith("order_id")) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length >= 2) {
            String customerId = fields[1].trim();

            // Emit customer_id with marker to indicate it comes from orders
            outputKey.set(customerId);
            context.write(outputKey, outputValue);

            context.getCounter("JOB1_STATS", "ORDER_RECORDS_PROCESSED").increment(1);
        }
    }
}