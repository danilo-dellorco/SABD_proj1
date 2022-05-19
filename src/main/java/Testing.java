import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.TaxiRow;

import java.util.ArrayList;
import java.util.List;

import static utils.tools.ParseRow;
import static utils.tools.promptEnterKey;

public class Testing {
    private static int count_null = 0;
    private static List query1_results = new ArrayList();
    //    private static String sparkURL = "spark://spark-master:7077";
//    private static String hdfsURL = "hdfs://hdfs-master:54310";
    private static String sparkURL = "local";
    private static String hdfsURL = "data";
    private static String dataset1_path = hdfsURL + "/yellow_tripdata_2021-12.parquet";
    private static String dataset2_path = hdfsURL + "/yellow_tripdata_2022-01.parquet";
    private static String dataset3_path = hdfsURL + "/yellow_tripdata_2022-02.parquet";

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master(sparkURL)
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        // QUERY 1
        /*
        query1_month(spark, dataset1_path);
        query1_month(spark, dataset2_path);
        query1_month(spark, dataset3_path);

        System.out.println("========================== QUERY 1 ==========================");
        for (int i = 0; i < 3; i++) {
            System.out.println(String.format("Computed Mean for Month %d: ", i) + query1_results.get(i));
        }
        System.out.println("=============================================================");
         */
        query3(spark, dataset1_path);
        promptEnterKey();
    }

    /*
    Average calculation on a monthly basis and on a
    subset of values tip/(total amount - toll amount)
     */
    public static void query1_month(SparkSession spark, String dataset) {
        JavaRDD<Row> rowRDD = spark.read().option("header", "false").parquet(dataset).toJavaRDD();
        JavaRDD<TaxiRow> taxiRows = rowRDD.map(r -> ParseRow(r));

        // Analisys of Month 1
        JavaRDD<Double> tips = taxiRows.map(t -> t.getTip_amount());
        JavaRDD<Double> total = taxiRows.map(t -> t.getTotal_amount());
        JavaRDD<Double> tolls = taxiRows.map(t -> t.getTolls_amount());

        double total_tips = tips.reduce((a, b) -> a + b);
        double total_amount = total.reduce((a, b) -> a + b);
        double tolls_amount = tolls.reduce((a, b) -> a + b);

        double mean = total_tips / (total_amount - tolls_amount);

        query1_results.add(mean);
    }

    /*
    Distribution of the number of trips, average tip and its
    standard deviation, the most popular payment
    method, in 1-hour slots
     */
    public static void query2() {

    }

    /*
    Identify the top-5 most popular DOLocationIDs (TLC
    Taxi Destination Zones), indicating for each one the
    average number of passengers and the mean and
    standard deviation of Fare_amount
    */
    public static void query3(SparkSession spark, String dataset) {
        JavaRDD<Row> rowRDD = spark.read().option("header", "false").parquet(dataset).toJavaRDD();
        JavaRDD<TaxiRow> taxis = rowRDD.map(r -> ParseRow(r));

        try {
            JavaPairRDD<Long,Integer> occurrences = taxis.mapToPair(r->new Tuple2<>(r.getDOLocationID(),1));
            JavaPairRDD<Long, Integer> reduced = occurrences.reduceByKey((a, b) -> a + b);
            reduced.foreach(data -> {
                System.out.println("DOLocationID="+data._1() + " occurrences=" + data._2());
            });
        } catch(Exception e){
            //Skip rows without getDOLocationID
        }
    }
}
