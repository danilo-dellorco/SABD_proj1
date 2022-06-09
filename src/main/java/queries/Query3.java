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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;
import utils.DateComparator;
import utils.KeyQ3;
import utils.Tools;
import utils.map.Zone;
import utils.valq.ValQ3;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static utils.Tools.getTimestamp;
import static utils.Tools.getTopFiveDestinations;

//TODO sistemare commenti

public class Query3 extends Query{
    List<Tuple2<String, List<Tuple2<Long, ValQ3>>>> results;

    public Query3(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection, String name) {
        super(spark, dataset, collection, name);
    }


    @Override
    public long execute() {
        Timestamp start = getTimestamp();
        // RDD:=[month,statistics]
        JavaPairRDD<KeyQ3, ValQ3> days = dataset.mapToPair(
                r -> new Tuple2<>(new KeyQ3(Tools.getDay(r.getTimestamp(1)), r.getLong(3)),
                        new ValQ3(r.getDouble(9), r.getDouble(5), 1)));

        // RDD:=[location_id,statistics_aggr]
        JavaPairRDD<KeyQ3, ValQ3> reduced = days.reduceByKey((Function2<ValQ3, ValQ3, ValQ3>) (v1, v2) -> {
            Double pass = v1.getPassengers() + v2.getPassengers();
            Double fare = v1.getFare() + v2.getFare();
            Integer occ = v1.getOccurrences() + v2.getOccurrences();
            ValQ3 v = new ValQ3(pass, fare, occ);
            return v;
        });

        JavaPairRDD<KeyQ3, ValQ3> mean = reduced.mapToPair(
                r -> {
                    Integer num_occurrences = r._2().getOccurrences();
                    Double pass_mean = r._2().getPassengers() / num_occurrences;
                    Double fare_mean = r._2().getFare() / num_occurrences;

                    return new Tuple2<>(r._1(),
                            new ValQ3(pass_mean, fare_mean, num_occurrences));
                });

        // RDD:=[location_id,statistics_stddev_iteration]
        JavaPairRDD<KeyQ3, Tuple2<ValQ3, ValQ3>> joined = days.join(mean);

        JavaPairRDD<KeyQ3, ValQ3> iterations = joined.mapToPair(
                r -> {
                    Double fare_mean = r._2()._2().getFare();
                    Double fare_val = r._2()._1().getFare();
                    Double fare_dev = Math.pow((fare_val - fare_mean), 2);
                    r._2()._2().setFare_stddev(fare_dev);

                    return new Tuple2<>(r._1(), r._2()._2());
                });

        // RDD:=[location_id,statistics_stddev_aggregated]
        JavaPairRDD<KeyQ3, ValQ3> stddev_aggr = iterations.reduceByKey((Function2<ValQ3, ValQ3, ValQ3>) (v1, v2) -> {
            Double fare_total_stddev = v1.getFare_stddev() + v2.getFare_stddev();
            ValQ3 v = new ValQ3(v1.getPassengers(), v1.getFare(), v1.getOccurrences(), fare_total_stddev);
            return v;
        });

        // RDD:=[location_id,statistics_stddev]
        JavaPairRDD<KeyQ3, ValQ3> deviation = stddev_aggr.mapToPair(
                r -> {
                    Double fare_mean = r._2().getFare();
                    Integer n = r._2().getOccurrences();
                    Double fare_dev = Math.sqrt(r._2().getFare_stddev() / n);
                    Double pass_mean = r._2().getPassengers();
                    ValQ3 v = new ValQ3(pass_mean, fare_mean, n, fare_dev);
                    return new Tuple2<>(r._1(), v);
                });

        JavaPairRDD<String, Iterable<Tuple2<KeyQ3,ValQ3>>> grouped = deviation.groupBy((Function<Tuple2<KeyQ3,ValQ3>, String>) r -> r._1().getDay());
        System.out.printf("Grouped: %d\n",grouped.count());

        // [(Mese,Destinazione), statistiche]
        // Tuple2< Tuple2<String,Long> ,ValQ3 >>
        JavaPairRDD<String, List<Tuple2<Long,ValQ3>>> top_destinations = grouped.mapToPair(r ->
                new Tuple2<>(
                        r._1(),
                        getTopFiveDestinations(r._2())
                ));

        results = top_destinations.sortByKey(new DateComparator()).collect();
        Timestamp end = getTimestamp();
        return end.getTime()-start.getTime();
    }

    // TODO finire
    @Override
    public long writeResultsOnMongo() {
        Timestamp start  =getTimestamp();
        for (Tuple2<String, List<Tuple2<Long,ValQ3>>> r : results) {
            String day = r._1();
            List<String> zones = new ArrayList<>();
            List<Double> passMeans = new ArrayList<>();
            List<Double> fareMeans = new ArrayList<>();
            List<Double> fareDevs = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                ValQ3 stats = r._2().get(i)._2();
                Integer zoneId = Math.toIntExact(r._2().get(i)._1());
                zones.add(Zone.zoneMap.get(zoneId));
                passMeans.add(stats.getPassengers());
                fareMeans.add(stats.getFare());
                fareDevs.add(stats.getFare_stddev());
            }
            //YYYY-MM-DD, DO1, DO2, ..., DO5, avg pax DO1, ..., avg pax DO5, avg fare DO1, ...,
            // * avg fare DO5, stddev fare DO1, ..., stddev fare DO5
            Document document = new Document();
            document.append("YYYY-MM-DD", day);
            for (int i = 0; i < 5; i++) {
                document.append("DO"+(i+1), zones.get(i));
            }
            for (int i = 0; i < 5; i++) {
                document.append("avg pax DO"+(i+1), passMeans.get(i));
            }
            for (int i = 0; i < 5; i++) {
                document.append("avg fare DO"+(i+1), fareMeans.get(i));
            }
            for (int i = 0; i < 5; i++) {
                document.append("stddev fare DO"+(i+1), fareDevs.get(i));
            }
            collection.insertOne(document);
        }
        Timestamp end = getTimestamp();
        return end.getTime()-start.getTime();
    }

    @Override
    public long writeResultsOnCSV() {
        Timestamp start = getTimestamp();
        super.writeResultsOnCSV();
        Timestamp end = getTimestamp();
        return end.getTime()-start.getTime();
    }
}
