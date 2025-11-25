package ru.example.repartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CompositeJoinReducer extends Reducer<CompositeKey, TaggedValue, Text, Text> {

    private static final int CUSTOMER_SOURCE = 1;
    private static final int ORDER_SOURCE = 2;
    private static final int PRODUCT_SOURCE = 3;

    @Override
    protected void reduce(CompositeKey key, Iterable<TaggedValue> values, Context context)
            throws IOException, InterruptedException {

        String joinKey = key.getJoinKey();
        List<String> customerRecords = new ArrayList<>();
        List<String> productRecords = new ArrayList<>();
        List<String> orderCustomerRecords = new ArrayList<>();
        List<String> orderProductRecords = new ArrayList<>();

        // Separate records by source type
        for (TaggedValue value : values) {
            int sourceIndex = value.getSourceIndex();
            String data = value.getData();

            switch (sourceIndex) {
                case CUSTOMER_SOURCE:
                    customerRecords.add(data);
                    break;
                case ORDER_SOURCE:
                    if (data.startsWith("CUSTOMER_JOIN:")) {
                        orderCustomerRecords.add(data.substring("CUSTOMER_JOIN:".length()));
                    } else if (data.startsWith("PRODUCT_JOIN:")) {
                        orderProductRecords.add(data.substring("PRODUCT_JOIN:".length()));
                    }
                    break;
                case PRODUCT_SOURCE:
                    productRecords.add(data);
                    break;
            }
        }

        // Perform the composite join logic

        // Join 1: Customers + Orders
        if (!customerRecords.isEmpty() && !orderCustomerRecords.isEmpty()) {
            for (String customerData : customerRecords) {
                for (String orderData : orderCustomerRecords) {
                    String[] orderFields = orderData.split(",");
                    String enrichedRecord = String.join(",",
                            joinKey,                    // customer_id
                            customerData,               // customer details
                            orderFields[0],             // order_id
                            orderFields[1],             // product_id
                            orderFields[2],             // order_date
                            orderFields[3],             // quantity
                            orderFields[4]              // total_amount
                    );
                    context.write(new Text("CUSTOMER_ORDER_JOIN"), new Text(enrichedRecord));
                    context.getCounter("REDUCER_STATS", "CUSTOMER_ORDER_JOINS").increment(1);
                }
            }
        }

        // Join 2: Products + Orders
        if (!productRecords.isEmpty() && !orderProductRecords.isEmpty()) {
            for (String productData : productRecords) {
                for (String orderData : orderProductRecords) {
                    String[] orderFields = orderData.split(",");
                    String enrichedRecord = String.join(",",
                            joinKey,                    // product_id
                            productData,                // product details
                            orderFields[0],             // order_id
                            orderFields[1],             // customer_id (from order)
                            orderFields[2],             // order_date
                            orderFields[3],             // quantity
                            orderFields[4]              // total_amount
                    );
                    context.write(new Text("PRODUCT_ORDER_JOIN"), new Text(enrichedRecord));
                    context.getCounter("REDUCER_STATS", "PRODUCT_ORDER_JOINS").increment(1);
                }
            }
        }

        // Join 3: Full composite join (Customers + Orders + Products)
        if (!customerRecords.isEmpty() && !orderCustomerRecords.isEmpty() && !productRecords.isEmpty()) {
            // This would require additional logic to correlate the joins
            // For simplicity, we're doing the two-way joins above
        }
    }
}