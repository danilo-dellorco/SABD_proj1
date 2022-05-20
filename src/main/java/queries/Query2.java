/**
 *     Distribution of the number of trips, average tip and its
 *     standard deviation, the most popular payment
 *     method, in 1-hour slots
 */

package queries;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.TaxiRow;
import java.util.ArrayList;
import java.util.List;
import static utils.Tools.ParseRow;

public class Query2 extends Query{
    public int HOUR_OF_DAY = 24;
    private static List query2_mean = new ArrayList();
    private static List<Tuple2<Integer,Long>> query2_methods = new ArrayList();

    public Query2(SparkSession spark, JavaRDD<Row> dataset) {
        super(spark, dataset);
    }

    public void execute() {
        long[] payments = new long[24];
        JavaRDD<TaxiRow> trips = dataset.map(r -> ParseRow(r));
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

    @Override
    public void print() {

    }
}
