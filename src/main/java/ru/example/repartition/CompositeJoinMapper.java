package ru.example.repartition;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CompositeJoinMapper extends Mapper<LongWritable, Text, CompositeKey, TaggedValue> {

    private String filename;
    private static final int CUSTOMER_SOURCE = 1;
    private static final int ORDER_SOURCE = 2;
    private static final int PRODUCT_SOURCE = 3;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Determine which file we're processing
        Path path = MapperUtils.getPath(context.getInputSplit());
        filename = path.getName();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip headers
        if (line.startsWith("customer_id") || line.startsWith("order_id") ||
                line.startsWith("product_id")) {
            return;
        }

        String[] fields = line.split(",");

        if (filename.contains("customers") && fields.length >= 4) {
            // Process customers data
            String customerId = fields[0].trim();
            String customerData = String.join(",",
                    fields[1], // name
                    fields[2], // country
                    fields[3]  // registration_date
            );

            CompositeKey compositeKey = new CompositeKey(customerId, CUSTOMER_SOURCE, customerId);
            TaggedValue taggedValue = new TaggedValue(CUSTOMER_SOURCE, customerData);

            context.write(compositeKey, taggedValue);
            context.getCounter("MAPPER_STATS", "CUSTOMER_RECORDS").increment(1);

        } else if (filename.contains("orders") && fields.length >= 6) {
            // Process orders data
            String orderId = fields[0].trim();
            String customerId = fields[1].trim();
            String productId = fields[2].trim();
            String orderData = String.join(",",
                    orderId,
                    productId,
                    fields[3], // order_date
                    fields[4], // quantity
                    fields[5]  // total_amount
            );

            // Emit for customer join
            CompositeKey customerKey = new CompositeKey(customerId, ORDER_SOURCE, orderId);
            TaggedValue customerValue = new TaggedValue(ORDER_SOURCE, "CUSTOMER_JOIN:" + orderData);
            context.write(customerKey, customerValue);

            // Emit for product join
            CompositeKey productKey = new CompositeKey(productId, ORDER_SOURCE, orderId);
            TaggedValue productValue = new TaggedValue(ORDER_SOURCE, "PRODUCT_JOIN:" + orderData);
            context.write(productKey, productValue);

            context.getCounter("MAPPER_STATS", "ORDER_RECORDS").increment(1);

        } else if (filename.contains("products") && fields.length >= 5) {
            // Process products data
            String productId = fields[0].trim();
            String productData = String.join(",",
                    fields[1], // product_name
                    fields[2], // category
                    fields[3], // price
                    fields[4]  // supplier
            );

            CompositeKey compositeKey = new CompositeKey(productId, PRODUCT_SOURCE, productId);
            TaggedValue taggedValue = new TaggedValue(PRODUCT_SOURCE, productData);

            context.write(compositeKey, taggedValue);
            context.getCounter("MAPPER_STATS", "PRODUCT_RECORDS").increment(1);
        }
    }
}