/**
 *     Distribution of the number of trips, average tip and its
 *     standard deviation, the most popular payment
 *     method, in 1-hour slots
 */

package queries;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.TaxiRow;
import utils.ValQ2;
import utils.ValQ3;

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
        JavaRDD<TaxiRow> trips = dataset.map(r -> ParseRow(r));
        // TODO .cache()

        // Total tips in every hour slot: (Hour, tips), we assume people pay on arrival
        JavaPairRDD<Integer, ValQ2> aggregated = trips.mapToPair(r ->
                    new Tuple2<>(r.getTpep_dropoff_datetime().toLocalDateTime().getHour(),
                    new ValQ2(r.getTip_amount(), r.getPayment_type(),1)));

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

        deviation.sortByKey(false).foreach((VoidFunction<Tuple2<Integer, ValQ2>>) r -> System.out.println(r.toString()));


















/*        // Total methods used in every hour slot: (occurrences, method_type)
        JavaPairRDD<Integer, Long> types = trips.mapToPair(row ->
                        new Tuple2<>(row.getTpep_dropoff_datetime().toLocalDateTime().getHour(), row.getPayment_type()))
//                .sortByKey(true).cache();   // caching because we use multiple time this RDD
        for (int i=0; i<HOUR_OF_DAY; i++) {
            int hour = i;
            JavaPairRDD<Integer, Long> methods = types.filter(row -> row._1().equals(hour))
                    .mapToPair(row -> new Tuple2<>(row._2(), 1)).reduceByKey(Integer::sum)
                    .mapToPair(row -> new Tuple2<>(row._2(), row._1())).sortByKey(false);
            methods.foreach(row -> System.out.println("Hour: " + hour + " Method:" + row._2()
                    + " | Occurrences: " + row._1()));
            List<Tuple2<Integer,Long>> top = methods.take(1);
            query2_methods.add(top.get(0));
        }*/
    }

    @Override
    public void print() {

    }
}
