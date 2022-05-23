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
import utils.Payments;
import utils.TaxiRow;
import utils.ValQ2;

import java.util.ArrayList;
import java.util.List;

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

        // RDD:=[time_slot,statistics]
        JavaPairRDD<Integer, ValQ2> aggregated = trips.mapToPair(r ->
                    new Tuple2<>(r.getTpep_dropoff_datetime().toLocalDateTime().getHour(),
                    new ValQ2(r.getTip_amount(), r.getPayment_type(),1))).sortByKey();
        //TODO .cache() credo


        /**
         * Calcolo del metodo di pagamento più diffuso per ogni fascia oraria
         */

        // RDD:=[(time_slot,payment),1]
        JavaPairRDD<Tuple2<Integer, Long>,Integer> aggr_pay = aggregated.mapToPair(r ->
                new Tuple2<>(
                        new Tuple2 <>(
                                r._1(),
                                r._2().getPayment_type())
                ,1));

        // RDD:=[(time_slot,payment),occurrences]
        JavaPairRDD<Tuple2<Integer, Long>,Integer> red_pay = aggr_pay.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> {
            Integer occ = v1+ v2;
            return occ;
        });

        // RDD:=[time_slot,{((time_slot,payment),occurrences)...}]
        JavaPairRDD<Integer, Iterable<Tuple2<Tuple2<Integer, Long>, Integer>>> grouped = red_pay.groupBy((Function<Tuple2<Tuple2<Integer, Long>, Integer>, Integer>) r -> r._1()._1());
//        grouped.foreach((VoidFunction<Tuple2<Integer, Iterable<Tuple2<Tuple2<Integer, Long>, Integer>>>>) r-> System.out.println(r.toString()));

        // RDD:=[time_slot,(top_payment,occurrences)]
        JavaPairRDD<Integer,Tuple2<Long,Integer>> top_payments = grouped.mapToPair(r ->
                new Tuple2<>(
                        r._1(),
                        getMostFrequentFromIterable(r._2())
                ));

        /**
         * Calcolo della media e deviazione standard di 'tips' per ogni fascia oraria
         */

        // RDD:=[time_slot,statistics_occurrences]
        JavaPairRDD<Integer, ValQ2> reduced = aggregated.reduceByKey((Function2<ValQ2, ValQ2, ValQ2>) (v1, v2) -> {
            Double tips = v1.getTips() + v2.getTips();
            Integer occ = v1.getOccurrences() + v2.getOccurrences();
            ValQ2 v = new ValQ2(tips, occ);
            return v;
        });

        // RDD:=[time_slot,statistics_mean]
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

        // RDD:=[time_slot,statistics_deviation_it]
        JavaPairRDD<Integer, ValQ2> iterations = joined.mapToPair(
                r -> {
                    Double tips_mean = r._2()._2().getTips();
                    Double tip_val = r._2()._1().getTips();
                    Double tips_dev = Math.pow((tip_val - tips_mean), 2);
                    r._2()._2().setTips_stddev(tips_dev);

                    return new Tuple2<>(r._1(), r._2()._2());
                });

        // RDD:=[time_slot,statistics_deviation_sum]
        JavaPairRDD<Integer, ValQ2> stddev_aggr = iterations.reduceByKey((Function2<ValQ2, ValQ2, ValQ2>) (v1, v2) -> {
            Double tips_total_stddev = v1.getTips_stddev() + v2.getTips_stddev();
            ValQ2 v = new ValQ2(v1.getTips(), v1.getOccurrences(), v1.getPayment_type(), tips_total_stddev);
            return v;
        });
//        stddev_aggr.foreach((VoidFunction<Tuple2<Integer, ValQ2>>) r -> System.out.println(r.toString()));

        // RDD:=[time_slot,statistics_deviation]
        JavaPairRDD<Integer, ValQ2> deviation = stddev_aggr.mapToPair(
                r -> {
                    Double tips_mean = r._2().getTips();
                    Integer n = r._2().getOccurrences();
                    Double tips_dev = Math.sqrt(r._2().getTips_stddev() / n);
                    ValQ2 v = new ValQ2(tips_mean, n, tips_dev);
                    return new Tuple2<>(r._1(), v);
                });

        // RDD:=[time_slot,(statistics,(top_payment,occurrences)))]
        JavaPairRDD<Integer, Tuple2<ValQ2, Tuple2<Long, Integer>>> final_joined = deviation.sortByKey()
                .join(top_payments)
                .sortByKey();


        // CSV LINE := time_slot, tips_mean, tips_dev, top_payment_type, top_payment_name, top_payment_tot
        JavaRDD<String> result = final_joined.map((Function<Tuple2<Integer, Tuple2<ValQ2, Tuple2<Long, Integer>>>, String>)
                                r -> {
            Integer id = Math.toIntExact(r._2()._2()._1());
            String name =  Payments.staticMap.get(id);

                    return String.format("%d;%f;%f;%d;%s;%d",r._1(),r._2()._1().getTips(),r._2()._1().getTips_stddev(),id,name,r._2()._2()._2());
                });

        result.saveAsTextFile("output/query2");
    }

    @Override
    public void print() {

    }
}
