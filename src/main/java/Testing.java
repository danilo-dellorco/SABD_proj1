import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.ApacheAccessLog;

import java.time.format.DateTimeFormatter;

public class Testing {
    static int count_null = 0;
    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        String reducedDataset = "data/reducedDataset.parquet";
        String dataset1 = "data/yellow_tripdata_2021-12.parquet";
        String dataset2 = "data/yellow_tripdata_2022-01.parquet";
        String dataset3 ="data/yellow_tripdata_2022-02.parquet";

        Dataset<Row> parquetDS = spark.read().option("header","false").parquet(dataset1);
        JavaRDD<Row> parquetRDD = parquetDS.toJavaRDD();

        JavaRDD<TaxiRow> taxiRows = parquetRDD
                .map(r->ParseRow(r));

        // Print all the TaxiRow RDDs
        taxiRows.foreach((VoidFunction<TaxiRow>) r->System.out.println(r.toString()));
        System.out.println("\n\n\nRows with null fields: " + count_null);
    }
    public static TaxiRow ParseRow(Row r) {
        TaxiRow t = new TaxiRow();
        try {
            t.VendorID = r.getLong(0);
            t.tpep_pickup_datetime = r.getTimestamp(1);
            t.tpep_dropoff_datetime = r.getTimestamp(2);
            t.passenger_count = r.getDouble(3);
            t.trip_distance = r.getDouble(4);
            t.RatecodeID = r.getDouble(5);
            t.store_and_fwd_flag = r.getString(6);
            t.PULocationID = r.getLong(7);
            t.DOLocationID = r.getLong(8);
            t.payment_type = r.getLong(9);
            t.fare_amount = r.getDouble(10);
            t.extra = r.getDouble(11);
            t.mta_tax = r.getDouble(12);
            t.tip_amount = r.getDouble(13);
            t.tolls_amount = r.getDouble(14);
            t.improvement_surcharge = r.getDouble(15);
            t.total_amount = r.getDouble(16);
            t.congestion_surcharge = r.getDouble(17);
            t.airport_fee = r.getDouble(18);
        } catch (NullPointerException e){
            count_null++;
            // Skip Null
        }

        return t;
    }
}
