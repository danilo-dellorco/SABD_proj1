import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function2;
import scala.*;
import utils.TaxiRow;
import utils.ValQ3;

import java.lang.Double;
import java.lang.Long;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static utils.Tools.ParseRow;
import static utils.Tools.promptEnterKey;

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

    //TODO vedere il caching
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

        // Analisys of the single Month
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

        JavaPairRDD<Long, ValQ3> occurrences = taxis.mapToPair(
                r -> new Tuple2<>(r.getDOLocationID(),
                        new ValQ3(r.getPassenger_count(), r.getFare_amount(), 1)));

        JavaPairRDD<Long, ValQ3> reduced = occurrences.reduceByKey((Function2<ValQ3, ValQ3, ValQ3>) (v1, v2) -> {
            Double pass = v1.getPassengers() + v2.getPassengers();
            Double fare = v1.getFare() + v2.getFare();
            Integer occ = v1.getOccurrences() + v2.getOccurrences();
            ValQ3 v = new ValQ3(pass, fare, occ);
            return v;
        });
        System.out.println("REDUCEEEEEED");
        reduced.sortByKey(false).foreach((VoidFunction<Tuple2<Long, ValQ3>>) r->System.out.println(r.toString()));

        JavaPairRDD<Long, ValQ3> statistics = reduced.mapToPair(
                r -> {
                    Integer num_occurrences = r._2().getOccurrences();
                    Double pass_mean = r._2().getPassengers() / num_occurrences;
                    Double fare_mean = r._2().getFare() / num_occurrences;

                    return new Tuple2<>(r._1(),
                            new ValQ3(pass_mean, fare_mean, num_occurrences));
                });
        statistics.foreach((VoidFunction<Tuple2<Long, ValQ3>>) r->System.out.println(r.toString()));

        JavaPairRDD<Long, Tuple2<ValQ3, ValQ3>> joined = occurrences.join(statistics);

        /*
        System.out.println("JOINEEEEEEED");
        joined.foreach((VoidFunction<Tuple2<Long, Tuple2<ValQ3, ValQ3>>>) r->System.out.println(r.toString()));
        */


        JavaPairRDD<Long, ValQ3> iterations = joined.mapToPair(
                r -> {
                    Double fare_mean = r._2()._2().getFare();
                    Double fare_val  = r._2()._1().getFare();
                    Double fare_dev  = Math.pow((fare_val-fare_mean),2);
                    r._2()._2().setFare_stddev(fare_dev);

                    return new Tuple2<>(r._1(), r._2()._2());
                });
        // iterations.foreach((VoidFunction<Tuple2<Long, ValQ3>>) r-> System.out.println(r));

        JavaPairRDD<Long, ValQ3> stddev_aggr = iterations.reduceByKey((Function2<ValQ3, ValQ3, ValQ3>) (v1, v2) -> {
            Double fare_total_stddev = v1.getFare_stddev()+v2.getFare_stddev();
            ValQ3 v = new ValQ3(v1.getPassengers(),v1.getFare(),v1.getOccurrences(),fare_total_stddev);
            return v;
        });

        JavaPairRDD<Long, ValQ3> deviation = stddev_aggr.mapToPair(
                r -> {
                    Double fare_mean = r._2().getFare();
                    Integer n = r._2().getOccurrences();
                    Double fare_dev  = Math.sqrt(r._2().getFare_stddev()/n);
                    Double pass_mean = r._2().getPassengers();
                    ValQ3 v = new ValQ3(pass_mean,fare_mean,n,fare_dev);
        // joined = JavaPairRDD<Long, Tuple2<ValQ3, ValQ3>
        //joined.foreach((VoidFunction<Tuple2<Long, Tuple2<ValQ3, ValQ3>>>) r -> System.out.println(r.toString()));

                    return new Tuple2<>(r._1(),v);
                });

        System.out.println("DEVIATIOOOOOON");
        deviation.foreach((VoidFunction<Tuple2<Long, ValQ3>>) r->System.out.println(r.toString()));
        deviation.foreach((VoidFunction<Tuple2<Long, ValQ3>>) r->System.out.println(r.toString()));

        JavaPairRDD<Integer, Tuple4<Long, Double, Double, Double>> sorted = deviation
                .mapToPair( x -> new Tuple2<>(x._2().getOccurrences(),
                        new Tuple4<>(x._1(), x._2().getPassengers(),
                        x._2().getFare(), x._2().getFare_stddev())))
                .sortByKey(false);
        System.out.println("\n\nSORTED");
        sorted.foreach((VoidFunction<Tuple2<Integer, Tuple4<Long, Double, Double, Double>>>) r-> System.out.println(r));


/*
        JavaPairRDD<ValQ3, Long> sorted = swapped.sortByKey((o1, o2) -> o1.getOccurrences().compareTo(o2.getOccurrences()));

       System.out.println("\n\nSORTED");
       sorted.foreach((VoidFunction<Tuple2<ValQ3, Long>>) r-> System.out.println(r));
/*
        List<Tuple2<ValQ3, Long>> top = sorted.take(5);
        System.out.println("\n\nTOP");
        int i =0;
        for (Tuple2<ValQ3, Long> t:top){
            System.out.println(String.format("%d) %d | %d",i,t._2(),t._1()));
            i++;
        }

 */
    }
}
