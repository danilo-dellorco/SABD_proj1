import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Testing {
    private static int count_null = 0;
    private static List query1_results = new ArrayList();
    private static String sparkURL = "spark://spark-master:7077";
    private static String hdfsURL = "hdfs://hdfs-master:54310";
//  private static String sparkURL = "local";
//  private static String hdfsURL = "data";
    private static String dataset1_path = hdfsURL+"/yellow_tripdata_2021-12.parquet";
    private static String dataset2_path = hdfsURL+"/yellow_tripdata_2022-01.parquet";
    private static String dataset3_path = hdfsURL+"/yellow_tripdata_2022-02.parquet";
    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .master(sparkURL)
                .appName("Java Spark SQL basic example")
                .getOrCreate();


        query1_month(spark,dataset1_path,1);
        query1_month(spark,dataset2_path,2);
        query1_month(spark,dataset3_path,3);
        System.out.println("========================== QUERY 1 ==========================");
        for (int i=0;i<3;i++){
            System.out.println(String.format("Computed Mean for Month %d: ",i)+query1_results.get(i));
        }
        System.out.println("=============================================================");
        promptEnterKey();


        // Print all the TaxiRow RDDs
        // taxiRows.foreach((VoidFunction<TaxiRow>) r->System.out.println(r.toString()));
        // tip/(total_amount-tolls_amount)



        //System.out.println("\n\n\nRows with null fields: " + count_null);
    }
    public static TaxiRow ParseRow(Row r) {
        TaxiRow t = new TaxiRow();
        try {
            t.setVendorID(r.getLong(0));
            t.setTpep_pickup_datetime(r.getTimestamp(1));
            t.setTpep_dropoff_datetime(r.getTimestamp(2));
            t.setPassenger_count(r.getDouble(3));
            t.setTrip_distance(r.getDouble(4));
            t.setRatecodeID(r.getDouble(5));
            t.setStore_and_fwd_flag(r.getString(6));
            t.setPULocationID(r.getLong(7));
            t.setDOLocationID(r.getLong(8));
            t.setPayment_type(r.getLong(9));
            t.setFare_amount(r.getDouble(10));
            t.setExtra(r.getDouble(11));
            t.setMta_tax(r.getDouble(12));
            t.setTip_amount(r.getDouble(13));
            t.setTolls_amount(r.getDouble(14));
            t.setImprovement_surcharge(r.getDouble(15));
            t.setTotal_amount(r.getDouble(16));
            t.setCongestion_surcharge(r.getDouble(17));
            t.setAirport_fee(r.getDouble(18));
        } catch (NullPointerException e) {
            count_null++;
            // Skip Null
        }

        return t;
    }

    public static void query1_month(SparkSession spark, String dataset, int month){
        JavaRDD<Row> rowRDD = spark.read().option("header","false").parquet(dataset).toJavaRDD();
        JavaRDD<TaxiRow> taxiRows = rowRDD.map(r->ParseRow(r));

        // Analisys of Month 1
        JavaRDD<Double> tips = taxiRows.map(t->t.getTip_amount());
        JavaRDD<Double> total = taxiRows.map(t->t.getTotal_amount());
        JavaRDD<Double> tolls = taxiRows.map(t->t.getTolls_amount());

        double total_tips = tips.reduce((a,b)->a+b);
        double total_amount = total.reduce((a,b)->a+b);
        double tolls_amount = tolls.reduce((a,b)->a+b);

        double mean = total_tips/(total_amount-tolls_amount);

        query1_results.add(mean);
        System.out.println( tips.getNumPartitions());
        System.out.println( total.getNumPartitions());
        System.out.println( tolls.getNumPartitions());
    }

    public static void promptEnterKey(){
        System.out.println("Running Spark WebUI on http://localhost:4040/jobs/");
        System.out.println("Press \"ENTER\" to end application...");
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
    }

}
