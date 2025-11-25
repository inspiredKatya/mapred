package ru.example.semijoin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.InputStreamReader;

public class CustomerFilterMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Set<String> activeCustomerIds = new HashSet<>();
    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load the distinct customer IDs from Job 1 output
        loadAllActiveCustomerIds(context);
    }

    private void loadAllActiveCustomerIds(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        // Path to Job 1 output directory (contains multiple part files)
        Path activeCustomersDir = new Path(context.getConfiguration()
                .get("active.customers.dir"));

        if (fs.exists(activeCustomersDir)) {
            // Get all part files in the directory
            FileStatus[] fileStatuses = fs.listStatus(activeCustomersDir);

            for (FileStatus status : fileStatuses) {
                Path filePath = status.getPath();
                String fileName = filePath.getName();

                // Read all part files (part-r-00000, part-r-00001, etc.)
                if (fileName.startsWith("part-")) {
                    context.getCounter("JOB2_STATS", "FILES_READ").increment(1);
                    loadCustomerIdsFromFile(fs, filePath, context);
                }
            }

            context.getCounter("JOB2_STATS", "ACTIVE_IDS_LOADED").setValue(activeCustomerIds.size());
            System.out.println("Loaded " + activeCustomerIds.size() + " active customer IDs from " +
                    fileStatuses.length + " part files");
        } else {
            throw new IOException("Active customers directory not found: " + activeCustomersDir);
        }
    }

    private void loadCustomerIdsFromFile(FileSystem fs, Path filePath, Context context)
            throws IOException {

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(filePath)))) {

            String line;
            int linesRead = 0;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+");
                if (parts.length >= 1) {
                    activeCustomerIds.add(parts[0].trim());
                    linesRead++;
                }
            }
            context.getCounter("JOB2_STATS", "LINES_READ_FROM_" + filePath.getName())
                    .setValue(linesRead);
        }
    }
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header
        if (line.startsWith("customer_id")) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length >= 6) {
            String customerId = fields[0].trim();

            // Semi-join: Only emit if customer exists in active customers set
            if (activeCustomerIds.contains(customerId)) {
                String customerData = String.join(",",
                        fields[1], // name
                        fields[2], // email
                        fields[3], // country
                        fields[4], // registration_date
                        fields[5]  // segment
                );

                outputKey.set(customerId);
                outputValue.set(customerData);
                context.write(outputKey, outputValue);

                context.getCounter("JOB2_STATS", "ACTIVE_CUSTOMERS_FOUND").increment(1);
            } else {
                context.getCounter("JOB2_STATS", "INACTIVE_CUSTOMERS_SKIPPED").increment(1);
            }
        }
    }
}