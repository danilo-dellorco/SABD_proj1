/**
 * Per ogni giorno, identificare le 5 zone di destinazione (DOLocationID) più popolari (in ordine de-
 * crescente), indicando per ciascuna di esse il numero medio di passeggeri, la media e la deviazione
 * standard della tariffa pagata (Fare amount). Nell’output della query, indicare il nome della zona
 * (TLC Taxi Destination Zone) anziché il codice numerico.
 *
 * Esempio di output:
 * # header: YYYY-MM-DD, DO1, DO2, ..., DO5, avg pax DO1, ..., avg pax DO5, avg fare DO1, ...,
 * avg fare DO5, stddev fare DO1, ..., stddev fare DO5
 * 2022-02-01, Queens - JFK Airport, Queens - La
 */

package queries;

import com.mongodb.client.MongoCollection;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.DateComparator;
import utils.Tools;
import utils.valq.ValQ3;

import java.util.Comparator;
import java.util.List;

import static utils.Tools.getMostFrequentPayment;
import static utils.Tools.getTopFiveDestinations;

public class Query3 extends Query{

    public Query3(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection, String name) {
        super(spark, dataset, collection, name);
    }


    @Override
    public void execute() {
        // RDD:=[month,statistics]
        JavaPairRDD<Tuple2<String,Long>, ValQ3> days = dataset.mapToPair(
                r -> new Tuple2<>(new Tuple2<>(Tools.getDay(r.getTimestamp(0)), r.getLong(1)),
                        new ValQ3(r.getDouble(7), r.getDouble(3), 1)));

        System.out.printf("Dataset: %d\n",days.count());

        // TODO aggregare le occorrenze e le statistiche di ogni cosa nel suo time slot e poi group by.

        // RDD:=[location_id,statistics_aggr]
        JavaPairRDD<Tuple2<String,Long>, ValQ3> reduced = days.reduceByKey((Function2<ValQ3, ValQ3, ValQ3>) (v1, v2) -> {
            Double pass = v1.getPassengers() + v2.getPassengers();
            Double fare = v1.getFare() + v2.getFare();
            Integer occ = v1.getOccurrences() + v2.getOccurrences();
            ValQ3 v = new ValQ3(pass, fare, occ);
            return v;
        });
        System.out.printf("Reduced: %d\n",reduced.count());

        JavaPairRDD<Tuple2<String,Long>, ValQ3> mean = reduced.mapToPair(
                r -> {
                    Integer num_occurrences = r._2().getOccurrences();
                    Double pass_mean = r._2().getPassengers() / num_occurrences;
                    Double fare_mean = r._2().getFare() / num_occurrences;

                    return new Tuple2<>(r._1(),
                            new ValQ3(pass_mean, fare_mean, num_occurrences));
                });

        System.out.printf("Mean: %d\n",mean.count());

        JavaPairRDD<String, Iterable<Tuple2<Tuple2<String,Long>,ValQ3>>> grouped = mean.groupBy((Function<Tuple2<Tuple2<String,Long>,ValQ3>, String>) r -> r._1()._1());
        System.out.printf("Grouped: %d\n",grouped.count());

        // [(Mese,Destinazione), statistiche]
        // Tuple2< Tuple2<String,Long> ,ValQ3 >>
        JavaPairRDD<String, List<Tuple2<Long,ValQ3>>> top_destinations = grouped.mapToPair(r ->
                new Tuple2<>(
                        r._1(),
                        getTopFiveDestinations(r._2())
                ));

        top_destinations
                .sortByKey(Comparator.<String>naturalOrder())
                .foreach((VoidFunction<Tuple2<String, List<Tuple2<Long, ValQ3>>>>) r-> System.out.println(r));
//        top_destinations.foreach((VoidFunction<Tuple2<String, List<Tuple2<Long, ValQ3>>>>) r-> System.out.println(r));


//        // RDD:=[location_id,statistics_mean]
//        JavaPairRDD<Long, ValQ3> statistics = reduced.mapToPair(
//                r -> {
//                    Integer num_occurrences = r._2().getOccurrences();
//                    Double pass_mean = r._2().getPassengers() / num_occurrences;
//                    Double fare_mean = r._2().getFare() / num_occurrences;
//
//                    return new Tuple2<>(r._1(),
//                            new ValQ3(pass_mean, fare_mean, num_occurrences));
//                });
//
//        // RDD:=[location_id,statistics_stdev_iteration]
//        JavaPairRDD<Long, Tuple2<ValQ3, ValQ3>> joined = days.join(statistics);
//        JavaPairRDD<Long, ValQ3> iterations = joined.mapToPair(
//                r -> {
//                    Double fare_mean = r._2()._2().getFare();
//                    Double fare_val = r._2()._1().getFare();
//                    Double fare_dev = Math.pow((fare_val - fare_mean), 2);
//                    r._2()._2().setFare_stddev(fare_dev);
//
//                    return new Tuple2<>(r._1(), r._2()._2());
//                });
//
//        // RDD:=[location_id,statistics_stdev_aggregated]
//        JavaPairRDD<Long, ValQ3> stddev_aggr = iterations.reduceByKey((Function2<ValQ3, ValQ3, ValQ3>) (v1, v2) -> {
//            Double fare_total_stddev = v1.getFare_stddev() + v2.getFare_stddev();
//            ValQ3 v = new ValQ3(v1.getPassengers(), v1.getFare(), v1.getOccurrences(), fare_total_stddev);
//            return v;
//        });
//
//        // RDD:=[location_id,statistics_stdev]
//        JavaPairRDD<Long, ValQ3> deviation = stddev_aggr.mapToPair(
//                r -> {
//                    Double fare_mean = r._2().getFare();
//                    Integer n = r._2().getOccurrences();
//                    Double fare_dev = Math.sqrt(r._2().getFare_stddev() / n);
//                    Double pass_mean = r._2().getPassengers();
//                    ValQ3 v = new ValQ3(pass_mean, fare_mean, n, fare_dev);
//                    return new Tuple2<>(r._1(), v);
//                });
//
//        // Swap della chiave con il numero di occorrenze
//        // RDD:=[occurrences,(location_id,passengers,fare,fare_stdev))]
//        JavaPairRDD<Integer, Tuple4<Long, Double, Double, Double>> sorted = deviation
//                .mapToPair( x ->
//                        new Tuple2<>(
//                                x._2().getOccurrences(),
//                                new Tuple4<>(
//                                    x._1(),
//                                    x._2().getPassengers(),
//                                    x._2().getFare(),
//                                    x._2().getFare_stddev())))
//                .sortByKey(false);
//
//        // Top 5 dei risultati in base al numero di occorrenze
//        List<Tuple2<Integer, Tuple4<Long, Double, Double, Double>>> top = sorted.take(5);
//
//        /**
//         * Salvataggio dei risultati su mongodb
//         */
//        for (Tuple2<Integer, Tuple4<Long, Double, Double, Double>> t : top) {
//            Integer zoneId = Math.toIntExact(t._2()._1());
//            String zoneName =  Zone.zoneMap.get(zoneId);
//            Integer occ = t._1();
//
//            Document document = new Document();
//            document.append("zone_id", zoneId);
//            document.append("zone_name", zoneName);
//            document.append("occurrences", occ);
//
//            collection.insertOne(document);
//        }
    }
}
