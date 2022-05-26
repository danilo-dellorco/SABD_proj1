/**
 *     Identify the top-5 most popular DOLocationIDs (TLC
 *     Taxi Destination Zones), indicating for each one the
 *     average number of passengers and the mean and
 *     standard deviation of Fare_amount
 */

package queries;

import com.mongodb.client.MongoCollection;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;
import scala.Tuple4;
import utils.YellowTaxiRow;
import utils.ValQ3;
import utils.Zone;

import java.util.List;

import static utils.Tools.ParseRow;

public class Query4 extends Query{

    public Query4(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection, String name) {
        super(spark, dataset, collection, name);
    }


    @Override
    public void execute() {
        //TODO la fase di filter forse va fatta nel pre-processamento rimuovendo le righe vuote
        JavaRDD<YellowTaxiRow> taxis = dataset.map(r -> ParseRow(r)).filter(v1->v1.getDOLocationID()!=0);

        // RDD:=[location_id,statistics]
        JavaPairRDD<Long, ValQ3> aggregated = taxis.mapToPair(
                r -> new Tuple2<>(r.getDOLocationID(),
                     new ValQ3(r.getPassenger_count(), r.getFare_amount(), 1)));

        // RDD:=[location_id,statistics_aggr]
        JavaPairRDD<Long, ValQ3> reduced = aggregated.reduceByKey((Function2<ValQ3, ValQ3, ValQ3>) (v1, v2) -> {
            Double pass = v1.getPassengers() + v2.getPassengers();
            Double fare = v1.getFare() + v2.getFare();
            Integer occ = v1.getOccurrences() + v2.getOccurrences();
            ValQ3 v = new ValQ3(pass, fare, occ);
            return v;
        });

        // RDD:=[location_id,statistics_mean]
        JavaPairRDD<Long, ValQ3> statistics = reduced.mapToPair(
                r -> {
                    Integer num_occurrences = r._2().getOccurrences();
                    Double pass_mean = r._2().getPassengers() / num_occurrences;
                    Double fare_mean = r._2().getFare() / num_occurrences;

                    return new Tuple2<>(r._1(),
                            new ValQ3(pass_mean, fare_mean, num_occurrences));
                });

        // RDD:=[location_id,statistics_stdev_iteration]
        JavaPairRDD<Long, Tuple2<ValQ3, ValQ3>> joined = aggregated.join(statistics);
        JavaPairRDD<Long, ValQ3> iterations = joined.mapToPair(
                r -> {
                    Double fare_mean = r._2()._2().getFare();
                    Double fare_val = r._2()._1().getFare();
                    Double fare_dev = Math.pow((fare_val - fare_mean), 2);
                    r._2()._2().setFare_stddev(fare_dev);

                    return new Tuple2<>(r._1(), r._2()._2());
                });

        // RDD:=[location_id,statistics_stdev_aggregated]
        JavaPairRDD<Long, ValQ3> stddev_aggr = iterations.reduceByKey((Function2<ValQ3, ValQ3, ValQ3>) (v1, v2) -> {
            Double fare_total_stddev = v1.getFare_stddev() + v2.getFare_stddev();
            ValQ3 v = new ValQ3(v1.getPassengers(), v1.getFare(), v1.getOccurrences(), fare_total_stddev);
            return v;
        });

        // RDD:=[location_id,statistics_stdev]
        JavaPairRDD<Long, ValQ3> deviation = stddev_aggr.mapToPair(
                r -> {
                    Double fare_mean = r._2().getFare();
                    Integer n = r._2().getOccurrences();
                    Double fare_dev = Math.sqrt(r._2().getFare_stddev() / n);
                    Double pass_mean = r._2().getPassengers();
                    ValQ3 v = new ValQ3(pass_mean, fare_mean, n, fare_dev);
                    return new Tuple2<>(r._1(), v);
                });

        // Swap della chiave con il numero di occorrenze
        // RDD:=[occurrences,(location_id,passengers,fare,fare_stdev))]
        JavaPairRDD<Integer, Tuple4<Long, Double, Double, Double>> sorted = deviation
                .mapToPair( x ->
                        new Tuple2<>(
                                x._2().getOccurrences(),
                                new Tuple4<>(
                                    x._1(),
                                    x._2().getPassengers(),
                                    x._2().getFare(),
                                    x._2().getFare_stddev())))
                .sortByKey(false);

        // Top 5 dei risultati in base al numero di occorrenze
        List<Tuple2<Integer, Tuple4<Long, Double, Double, Double>>> top = sorted.take(5);

        /**
         * Salvataggio dei risultati su mongodb
         */
        for (Tuple2<Integer, Tuple4<Long, Double, Double, Double>> t : top) {
            Integer zoneId = Math.toIntExact(t._2()._1());
            String zoneName =  Zone.zoneMap.get(zoneId);
            Integer occ = t._1();

            Document document = new Document();
            document.append("zone_id", zoneId);
            document.append("zone_name", zoneName);
            document.append("occurrences", occ);

            collection.insertOne(document);

        }
    }
}
