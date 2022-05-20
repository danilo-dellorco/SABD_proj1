import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function2;
import scala.*;
import utils.TaxiRow;
import utils.ValQ3;

import java.lang.Double;
import java.lang.Long;
import java.util.ArrayList;
import java.util.List;

import static utils.Tools.ParseRow;
import static utils.Tools.promptEnterKey;

public class Testing {
    private static List query1_results = new ArrayList();
    private static List query2_mean = new ArrayList();
    private static List<Tuple2<Integer,Long>> query2_methods = new ArrayList();

    //    private static String sparkURL = "spark://spark-master:7077";
    //    private static String hdfsURL = "hdfs://hdfs-master:54310";
    private static String sparkURL = "local";
    private static String hdfsURL = "data";
    private static String dataset1_path = hdfsURL + "/yellow_tripdata_2021-12.parquet";
    private static String dataset2_path = hdfsURL + "/yellow_tripdata_2022-01.parquet";
    private static String dataset3_path = hdfsURL + "/yellow_tripdata_2022-02.parquet";
    public static int HOUR_OF_DAY = 24;

    //TODO vedere il caching per gli RDD riacceduti
    //TODO scrivere gli output su file HDFS
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master(sparkURL)
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        // QUERY 1
        query1_month(spark, dataset1_path);
        query1_month(spark, dataset2_path);
        query1_month(spark, dataset3_path);

        System.out.println("========================== QUERY 1 ==========================");
        for (int i = 0; i < 3; i++) {
            System.out.println(String.format("Computed Mean for Month %d: ", i) + query1_results.get(i));
        }
        System.out.println("=============================================================");

        query2(spark, dataset1_path);
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
    public static void query2(SparkSession spark, String dataset) {
        long[] payments = new long[24];
        JavaRDD<Row> rowRDD = spark.read().option("header", "false").parquet(dataset).toJavaRDD();
        JavaRDD<TaxiRow> trips = rowRDD.map(r -> ParseRow(r));
        // TODO .cache()

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

    /*
    Identify the top-5 most popular DOLocationIDs (TLC
    Taxi Destination Zones), indicating for each one the
    average number of passengers and the mean and
    standard deviation of Fare_amount
    */
    public static void query3(SparkSession spark, String dataset) {
        //TODO la fase di filter forse va fatta nel pre-processamento rimuovendo le righe vuote
        JavaRDD<Row> rowRDD = spark.read().option("header", "false").parquet(dataset).toJavaRDD();
        JavaRDD<TaxiRow> taxis = rowRDD.map(r -> ParseRow(r)).filter(v1->v1.getDOLocationID()!=0);

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

        /*
        System.out.println("REDUCEEEEEED");
        reduced.sortByKey(false).foreach((VoidFunction<Tuple2<Long, ValQ3>>) r->System.out.println(r.toString()));
        */

        JavaPairRDD<Long, ValQ3> statistics = reduced.mapToPair(
                r -> {
                    Integer num_occurrences = r._2().getOccurrences();
                    Double pass_mean = r._2().getPassengers() / num_occurrences;
                    Double fare_mean = r._2().getFare() / num_occurrences;

                    return new Tuple2<>(r._1(),
                            new ValQ3(pass_mean, fare_mean, num_occurrences));
                });
        //statistics.foreach((VoidFunction<Tuple2<Long, ValQ3>>) r -> System.out.println(r.toString()));

        // Unisce ad ogni trip del taxi la media ed il numero di occorrenze totali
        JavaPairRDD<Long, Tuple2<ValQ3, ValQ3>> joined = occurrences.join(statistics);

        /*
        System.out.println("JOINEEEEEEED");
        joined.foreach((VoidFunction<Tuple2<Long, Tuple2<ValQ3, ValQ3>>>) r->System.out.println(r.toString()));
        */


        JavaPairRDD<Long, ValQ3> iterations = joined.mapToPair(
                r -> {
                    Double fare_mean = r._2()._2().getFare();
                    Double fare_val = r._2()._1().getFare();
                    Double fare_dev = Math.pow((fare_val - fare_mean), 2);
                    r._2()._2().setFare_stddev(fare_dev);

                    return new Tuple2<>(r._1(), r._2()._2());
                });
        // iterations.foreach((VoidFunction<Tuple2<Long, ValQ3>>) r-> System.out.println(r));

        JavaPairRDD<Long, ValQ3> stddev_aggr = iterations.reduceByKey((Function2<ValQ3, ValQ3, ValQ3>) (v1, v2) -> {
            Double fare_total_stddev = v1.getFare_stddev() + v2.getFare_stddev();
            ValQ3 v = new ValQ3(v1.getPassengers(), v1.getFare(), v1.getOccurrences(), fare_total_stddev);
            return v;
        });

        JavaPairRDD<Long, ValQ3> deviation = stddev_aggr.mapToPair(
                r -> {
                    Double fare_mean = r._2().getFare();
                    Integer n = r._2().getOccurrences();
                    Double fare_dev = Math.sqrt(r._2().getFare_stddev() / n);
                    Double pass_mean = r._2().getPassengers();
                    ValQ3 v = new ValQ3(pass_mean, fare_mean, n, fare_dev);
                    // joined = JavaPairRDD<Long, Tuple2<ValQ3, ValQ3>
                    // joined.foreach((VoidFunction<Tuple2<Long, Tuple2<ValQ3, ValQ3>>>) r -> System.out.println(r.toString()));

                    return new Tuple2<>(r._1(), v);
                });

        /*
        System.out.println("DEVIATIOOOOOON");
        deviation.foreach((VoidFunction<Tuple2<Long, ValQ3>>) r -> System.out.println(r.toString()));
        */
        JavaPairRDD<Integer, Tuple4<Long, Double, Double, Double>> sorted = deviation
                .mapToPair( x -> new Tuple2<>(x._2().getOccurrences(),
                        new Tuple4<>(x._1(), x._2().getPassengers(),
                        x._2().getFare(), x._2().getFare_stddev())))
                .sortByKey(false);
        //System.out.println("\n\nSORTED");
        //sorted.foreach((VoidFunction<Tuple2<Integer, Tuple4<Long, Double, Double, Double>>>) r-> System.out.println(r));

        System.out.println("========================== QUERY 3 ==========================");

        List<Tuple2<Integer, Tuple4<Long, Double, Double, Double>>> top = sorted.take(5);
        int i = 1;
        for (Tuple2<Integer, Tuple4<Long, Double, Double, Double>> t : top) {
            System.out.println(String.format("%d) %d | %d", i, t._2()._1(), t._1()));
            i++;
        }
        System.out.println("=============================================================");

    }
}