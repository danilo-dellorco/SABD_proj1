/**
 *     Identify the top-5 most popular DOLocationIDs (TLC
 *     Taxi Destination Zones), indicating for each one the
 *     average number of passengers and the mean and
 *     standard deviation of Fare_amount
 */

package queries;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple4;
import utils.TaxiRow;
import utils.ValQ3;
import utils.Zone;

import java.util.List;

import static utils.Tools.ParseRow;

public class Query3 extends Query{

    public Query3(SparkSession spark, JavaRDD<Row> dataset) {
        super(spark, dataset);
    }


    @Override
    public void execute() {
        //TODO la fase di filter forse va fatta nel pre-processamento rimuovendo le righe vuote
        JavaRDD<TaxiRow> taxis = dataset.map(r -> ParseRow(r)).filter(v1->v1.getDOLocationID()!=0);

        JavaPairRDD<Long, ValQ3> aggregated = taxis.mapToPair(
                r -> new Tuple2<>(r.getDOLocationID(),
                     new ValQ3(r.getPassenger_count(), r.getFare_amount(), 1)));

        JavaPairRDD<Long, ValQ3> reduced = aggregated.reduceByKey((Function2<ValQ3, ValQ3, ValQ3>) (v1, v2) -> {
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
        JavaPairRDD<Long, Tuple2<ValQ3, ValQ3>> joined = aggregated.join(statistics);

//        /*
        System.out.println("JOINEEEEEEED");
        joined.foreach((VoidFunction<Tuple2<Long, Tuple2<ValQ3, ValQ3>>>) r->System.out.println(r.toString()));
//        */


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


        List<Tuple2<Integer, Tuple4<Long, Double, Double, Double>>> top = sorted.take(5);
        System.out.println("========================== QUERY 3 ==========================");
        int i = 1;
        for (Tuple2<Integer, Tuple4<Long, Double, Double, Double>> t : top) {
            Integer id = Math.toIntExact(t._2()._1());
            String zoneName =  Zone.staticMap.get(id);
            Integer occ = t._1();
            System.out.println(String.format("%d° - %d | %d, %s", i,occ,id,zoneName));
            i++;
        }
        System.out.println("=============================================================");
    }

    @Override
    public void print() {

    }
}
