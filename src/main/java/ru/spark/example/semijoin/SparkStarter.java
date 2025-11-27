package ru.spark.example.semijoin;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SaveMode;

/*run configuration src/main/resources/semijoin/recent-orders.csv src/main/resources/semijoin/customers-big.csv build/semijoin-out
* spark-submit \
    --class SparkSemiJoinExample \
    --master local[*] \
    --conf spark.sql.adaptive.enabled=true \
    target/spark-semi-join-1.0-SNAPSHOT.jar \
    /path/to/orders.csv /path/to/customers.csv /output
 */
public class SparkStarter {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: SparkSemiJoinExample <orders> <customers> <output>");
            System.exit(1);
        }

        String ordersPath = args[0];
        String customersPath = args[1];
        String outputPath = args[2];

        // Initialize Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("Spark Semi-Join Example")
                .master("local[*]") // in memory
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.skew.enabled", "true")
                .getOrCreate();

        try {
            // Set log level to avoid excessive logging
            spark.sparkContext().setLogLevel("WARN");
            System.out.println("=== Starting Spark Semi-Join ===");

            // Run the semi-join analysis
            performSemiJoinAnalysis(spark, ordersPath, customersPath, outputPath);

            System.out.println("=== Spark Semi-Join Completed Successfully ===");

        } catch (Exception e) {
            System.err.println("Error during Spark execution: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }

    private static void performSemiJoinAnalysis(SparkSession spark,
                                                String ordersPath,
                                                String customersPath,
                                                String outputPath) {

        System.out.println("Loading datasets...");

        // Load customers dataset
        Dataset<Row> customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(customersPath)
                .cache(); // Cache for multiple operations

        // Load orders dataset
        Dataset<Row> orders = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(ordersPath)
                .cache();

        printDebugData(customers, orders);

        // Method 1: Using SQL-style semi-join
        System.out.println("\n=== Method 1: Using SQL Semi-Join ===");
        Dataset<Row> activeCustomersSQL = performSemiJoinSQL(spark, customers, orders);
        System.out.println("Active customers count (SQL method): " + activeCustomersSQL.count());
        activeCustomersSQL.show(10, false);

        // Method 2: Using DataFrame API with join and distinct
        System.out.println("\n=== Method 2: Using DataFrame API ===");
        Dataset<Row> activeCustomersDF = performSemiJoinDataFrame(customers, orders);
        System.out.println("Active customers count (DataFrame method): " + activeCustomersDF.count());
        activeCustomersDF.show(10, false);

        // Method 3: Using left-semi join explicitly
        System.out.println("\n=== Method 3: Using Left-Semi Join ===");
        Dataset<Row> activeCustomersLeftSemi = performLeftSemiJoin(customers, orders);

        System.out.println("Active customers count (Left-Semi method): " + activeCustomersLeftSemi.count());
        activeCustomersLeftSemi.show(10, false);

        // Additional analysis: Customer order statistics
        System.out.println("\n=== Customer Order Statistics ===");
        Dataset<Row> customerStats = performCustomerAnalysis(customers, orders);
        customerStats.show(10, false);

        // Save results
        System.out.println("\n=== Saving Results ===");
        activeCustomersLeftSemi
                .repartition(2) // Control output file count
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(outputPath + "/active_customers");

        customerStats
                .repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(outputPath + "/customer_stats");

        // Unpersist cached datasets
        customers.unpersist();
        orders.unpersist();

        System.out.println("Results saved to: " + outputPath);
    }

    private static void printDebugData(Dataset<Row> customers, Dataset<Row> orders) {
        System.out.println("=== Dataset Statistics ===");
        System.out.println("Customers count: " + customers.count());
        System.out.println("Orders count: " + orders.count());

        // Show schema
        System.out.println("Customers schema:");
        customers.printSchema();

        System.out.println("Orders schema:");
        orders.printSchema();

        // Show sample data
        System.out.println("Sample customers data:");
        customers.show(5, false);

        System.out.println("Sample orders data:");
        orders.show(5, false);
    }

    /**
     * Method 1: Using SQL query for semi-join
     */
    private static Dataset<Row> performSemiJoinSQL(SparkSession spark,
                                                   Dataset<Row> customers,
                                                   Dataset<Row> orders) {

        // Create temporary views
        customers.createOrReplaceTempView("customers");
        orders.createOrReplaceTempView("orders");

        // SQL query for semi-join (customers who have at least one order)
        String sqlQuery = " SELECT c.* FROM customers c WHERE EXISTS " +
                "( SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id )" +
                " ORDER BY c.customer_id";

        return spark.sql(sqlQuery);
    }

    /**
     * Method 2: Using DataFrame API with join and distinct
     */
    private static Dataset<Row> performSemiJoinDataFrame(Dataset<Row> customers,
                                                         Dataset<Row> orders) {

        // Get distinct customer IDs from orders
        Dataset<Row> activeCustomerIds = orders
                .select("customer_id")
                .distinct();

        // Join with customers to get full customer details
        return customers
                .join(activeCustomerIds,
                        customers.col("customer_id").equalTo(activeCustomerIds.col("customer_id")),
                        "inner")
                .select(customers.col("*"))
                .orderBy("customer_id");
    }

    /**
     * Method 3: Using left-semi join (most efficient for semi-join)
     */
    private static Dataset<Row> performLeftSemiJoin(Dataset<Row> customers,
                                                    Dataset<Row> orders) {

        return customers
                .join(orders,
                        customers.col("customer_id").equalTo(orders.col("customer_id")),
                        "left_semi") // This is the key - left_semi join
                .orderBy("customer_id");
    }

    /**
     * Additional analysis: Customer order statistics
     */
    private static Dataset<Row> performCustomerAnalysis(Dataset<Row> customers,
                                                        Dataset<Row> orders) {

        // Calculate order statistics per customer
        Dataset<Row> orderStats = orders
                .groupBy("customer_id")
                .agg(
                        count("order_id").as("total_orders"),
                        sum("total_amount").as("total_spent"),
                        avg("total_amount").as("avg_order_value"),
                        max("order_date").as("last_order_date")
                );

        // Join with customer details
        return customers
                .join(orderStats,
                        customers.col("customer_id").equalTo(orderStats.col("customer_id")),
                        "left") // Left join to include all customers
                .select(
                        customers.col("customer_id"),
                        customers.col("customer_name"),
                        customers.col("country"),
                        customers.col("registration_date"),
                        orderStats.col("total_orders"),
                        orderStats.col("total_spent"),
                        orderStats.col("avg_order_value"),
                        orderStats.col("last_order_date")
                )
                .withColumn("customer_status",
                        when(col("total_orders").isNull(), "INACTIVE")
                                .otherwise("ACTIVE")
                )
                .orderBy(col("total_spent").desc_nulls_last());
    }
}