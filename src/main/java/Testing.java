import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.TaxiRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Testing {
    private static List query1_results = new ArrayList();
    private static List query2_mean = new ArrayList();
    private static List<Tuple2<Integer,Long>> query2_methods = new ArrayList();
    public static int HOUR_OF_DAY = 24;
    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        String dataset1 = "data/yellow_tripdata_2021-12.parquet";
        String dataset2 = "data/yellow_tripdata_2022-01.parquet";
        String dataset3 ="data/yellow_tripdata_2022-02.parquet";

        query1(spark,dataset1);
        query1(spark,dataset2);
        query1(spark,dataset3);
        System.out.println("========================== QUERY 1 ==========================");
        for (int i=0;i<3;i++){
            System.out.println(String.format("Computed Mean for Month %d: ",i)+query1_results.get(i));
        }
        System.out.println("=============================================================");


        // Print all the TaxiRow RDDs
        // taxiRows.foreach((VoidFunction<TaxiRow>) r->System.out.println(r.toString()));
        // tip/(total_amount-tolls_amount)

        query2(spark, dataset1);
        System.out.println("========================== QUERY 2 ==========================");
        for (int i = 0; i < 24; i++) {
            System.out.println(String.format("Average Tip for Hour %d: ", i) + query2_mean.get(i));
        }
        System.out.println("\n\n");
        for (int i = 0; i < 24; i++) {
            System.out.println(String.format("Most popular Payment Method for Hour %d: ", i)
                    + query2_methods.get(i)._2() + " w/ occurrences: " + query2_methods.get(i)._1());
        }
        System.out.println("=============================================================");
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
    }

    //average calculation of the tip amount on monthly base
    public static void query1(SparkSession spark, String dataset) {
        JavaRDD<Row> rowRDD = spark.read().option("header","false").parquet(dataset).toJavaRDD();
        JavaRDD<TaxiRow> trips = rowRDD.map(TaxiRow::parseRow).cache();

        // Analysis of the specified month
        JavaRDD<Double> tips = trips.map(TaxiRow::getTip_amount);
        JavaRDD<Double> total = trips.map(TaxiRow::getTotal_amount);
        JavaRDD<Double> tolls = trips.map(TaxiRow::getTolls_amount);

        double total_tips = tips.reduce(Double::sum);
        double total_amount = total.reduce(Double::sum);
        double tolls_amount = tolls.reduce(Double::sum);

        double mean = total_tips/(total_amount-tolls_amount);
        query1_results.add(mean);
    }



    // average tip and its stdev (manca), most popular payment method, in 1-hour slots
    public static void query2(SparkSession spark, String dataset) {
        long[] payments = new long[24];
        JavaRDD<Row> rowRDD = spark.read().option("header", "false").parquet(dataset).toJavaRDD();
        JavaRDD<TaxiRow> trips = rowRDD.map(TaxiRow::parseRow).cache();

        // Total tips in every hour slot: (Hour, tips), we assume people pay on arrival
        JavaPairRDD<Integer, Double> tips = trips.mapToPair(row ->
                        new Tuple2<>(row.getTpep_dropoff_datetime().toLocalDateTime().getHour(), row.getTip_amount()))
                .sortByKey(true).cache(); // caching because we use multiple times this RDD
        JavaPairRDD<Integer, Double> total = tips.reduceByKey(Double::sum)
                .sortByKey(true).cache(); // caching because we use multiple times this RDD

        System.out.println("\nTotal tips in every hour slot:");
        total.foreach(row -> System.out.println("Hour:" + row._1() + " Tips:" + row._2()));

        // Total payments in every hour slot
        for (int i=0; i<HOUR_OF_DAY; i++) {
            int hour = i;
            payments[hour] = tips.filter(row -> row._1().equals(hour)).count();
            System.out.println("Hour:" + hour + " Payments:" + payments[hour]);
        }

        // Average tips in every hour slot: Total tips/Total payments
        List<Tuple2<Integer, Double>> tmp = total.collect();
        for (int i=0; i<HOUR_OF_DAY; i++) {
            double tip = tmp.get(i)._2();
            double mean = tip/payments[i];
            query2_mean.add(mean);
        }

        // Total methods used in every hour slot: (occurrences, method_type)
        JavaPairRDD<Integer, Long> types = trips.mapToPair(row ->
                        new Tuple2<>(row.getTpep_dropoff_datetime().toLocalDateTime().getHour(), row.getPayment_type()))
                .sortByKey(true).cache();   // caching because we use multiple time this RDD
        for (int i=0; i<HOUR_OF_DAY; i++) {
            int hour = i;
            JavaPairRDD<Integer, Long> methods = types.filter(row -> row._1().equals(hour))
                    .mapToPair(row -> new Tuple2<>(row._2(), 1)).reduceByKey(Integer::sum)
                    .mapToPair(row -> new Tuple2<>(row._2(), row._1())).sortByKey(false);
            methods.foreach(row -> System.out.println("Hour: " + hour + " Method:" + row._2()
                    + " | Occurrences: " + row._1()));
            List<Tuple2<Integer,Long>> top = methods.take(1);
            query2_methods.add(top.get(0));
        }
    }



    public static void query3(SparkSession spark, String dataset) {
        JavaRDD<Row> rowRDD = spark.read().option("header", "false").parquet(dataset).toJavaRDD();
        JavaRDD<TaxiRow> trips = rowRDD.map(TaxiRow::parseRow);

        // Total occurrences of the all locations: (DOLocationID, total)
        JavaPairRDD<Long, Integer> occurrences = trips.mapToPair(row -> new Tuple2<>(row.getDOLocationID(), 1));
        JavaPairRDD<Long, Integer> total = occurrences.reduceByKey(Integer::sum);

        System.out.println("Occurrences for all locations:");
        total.foreach(location -> {
            System.out.println("DOLocationID=" + location._1() + " occurrences=" + location._2());
        });

        // Switch location pairs to (total, DOLocationID) to sort in descending mode and take the top-5
        JavaPairRDD<Integer,Long> swap = total.mapToPair(row -> new Tuple2<>(row._2(), row._1()));
        JavaPairRDD<Integer,Long> sorted = swap.sortByKey(false);
        List<Tuple2<Integer,Long>> top = sorted.take(5);

        System.out.println("\nList of top-5:");
        for (Tuple2<Integer,Long> location : top) {
            System.out.printf("%d" + location._2() + " - " + location._1());
        }
    }
}