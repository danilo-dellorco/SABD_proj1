/**
 *     Distribution of the number of trips, average tip and its
 *     standard deviation, the most popular payment
 *     method, in 1-hour slots
 */

package queries;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.TaxiRow;
import utils.ValQ2;
import utils.ValQ3;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static utils.Tools.*;

public class Query2 extends Query{
    public int HOUR_OF_DAY = 24;
    private static List query2_mean = new ArrayList();
    private static List<Tuple2<Integer,Long>> query2_methods = new ArrayList();

    public Query2(SparkSession spark, JavaRDD<Row> dataset) {
        super(spark, dataset);
    }

    public void execute() {
        JavaRDD<TaxiRow> trips = dataset.map(r -> ParseRow(r));
        // TODO .cache()

        // Total tips in every hour slot: (Hour, tips), we assume people pay on arrival
        JavaPairRDD<Integer, ValQ2> aggregated = trips.mapToPair(r ->
                    new Tuple2<>(r.getTpep_dropoff_datetime().toLocalDateTime().getHour(),
                    new ValQ2(r.getTip_amount(), r.getPayment_type(),1))).sortByKey();
        //TODO .cache() credo

//        aggregated.foreach((VoidFunction<Tuple2<Integer, ValQ2>>) r-> System.out.println(r.toString()));

        JavaPairRDD<Tuple2<Integer, Long>,Integer> aggr_pay = aggregated.mapToPair(r ->
                new Tuple2<>(
                        new Tuple2 <>(
                                r._1(),
                                r._2().getPayment_type())
                ,1));

        JavaPairRDD<Tuple2<Integer, Long>,Integer> red_pay = aggr_pay.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> {
            Integer occ = v1+ v2;
            return occ;
        });

        //TODO prendere il max dall'iterable ed è finito
        JavaPairRDD<Integer, Iterable<Tuple2<Tuple2<Integer, Long>, Integer>>> grouped = red_pay.groupBy((Function<Tuple2<Tuple2<Integer, Long>, Integer>, Integer>) r -> r._1()._1());
//        grouped.foreach((VoidFunction<Tuple2<Integer, Iterable<Tuple2<Tuple2<Integer, Long>, Integer>>>>) r-> System.out.println(r.toString()));

        JavaPairRDD<Integer,Tuple2<Long,Integer>> rdd_boh = grouped.mapToPair(r ->
                new Tuple2<>(
                        r._1(),
                        getMostFrequentFromIterable(r._2())
                ));
//        rdd_boh.sortByKey().foreach((VoidFunction<Tuple2<Integer, Tuple2<Long, Integer>>>) r-> System.out.println(r.toString()));

        JavaPairRDD<Integer, ValQ2> reduced = aggregated.reduceByKey((Function2<ValQ2, ValQ2, ValQ2>) (v1, v2) -> {
            Double tips = v1.getTips() + v2.getTips();
            Integer occ = v1.getOccurrences() + v2.getOccurrences();
            ValQ2 v = new ValQ2(tips, occ);
            return v;
        });

        JavaPairRDD<Integer, ValQ2> statistics = reduced.mapToPair(
                r -> {
                    Integer num_occurrences = r._2().getOccurrences();
                    Double tips_mean = r._2().getTips() / num_occurrences;

                    return new Tuple2<>(r._1(),
                            new ValQ2(tips_mean, num_occurrences));
                });

        JavaPairRDD<Integer, Tuple2<ValQ2, ValQ2>> joined = aggregated.join(statistics);

//        System.out.println("JOINEEEEEEED");
//        joined.foreach((VoidFunction<Tuple2<Integer, Tuple2<ValQ2, ValQ2>>>) r->System.out.println(r.toString()));

        JavaPairRDD<Integer, ValQ2> iterations = joined.mapToPair(
                r -> {
                    Double tips_mean = r._2()._2().getTips();
                    Double tip_val = r._2()._1().getTips();
                    Double tips_dev = Math.pow((tip_val - tips_mean), 2);
                    r._2()._2().setTips_stddev(tips_dev);

                    return new Tuple2<>(r._1(), r._2()._2());
                });

        JavaPairRDD<Integer, ValQ2> stddev_aggr = iterations.reduceByKey((Function2<ValQ2, ValQ2, ValQ2>) (v1, v2) -> {
            Double tips_total_stddev = v1.getTips_stddev() + v2.getTips_stddev();
            ValQ2 v = new ValQ2(v1.getTips(), v1.getOccurrences(), v1.getPayment_type(), tips_total_stddev);
            return v;
        });
//        stddev_aggr.foreach((VoidFunction<Tuple2<Integer, ValQ2>>) r -> System.out.println(r.toString()));

        JavaPairRDD<Integer, ValQ2> deviation = stddev_aggr.mapToPair(
                r -> {
                    Double tips_mean = r._2().getTips();
                    Integer n = r._2().getOccurrences();
                    Double tips_dev = Math.sqrt(r._2().getTips_stddev() / n);
                    ValQ2 v = new ValQ2(tips_mean, n, tips_dev);
                    return new Tuple2<>(r._1(), v);
                });

//        deviation.sortByKey(false).foreach((VoidFunction<Tuple2<Integer, ValQ2>>) r -> System.out.println(r.toString()));

        JavaPairRDD<Integer, Tuple2<ValQ2, Tuple2<Long, Integer>>> final_joined = deviation.sortByKey().join(rdd_boh);
        final_joined.sortByKey().foreach((VoidFunction<Tuple2<Integer, Tuple2<ValQ2, Tuple2<Long, Integer>>>>) r-> System.out.println(r.toString()));
    }

    @Override
    public void print() {

    }
}
