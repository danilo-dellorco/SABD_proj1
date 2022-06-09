/**
 * Per ogni ora, calcolare la distribuzione in percentuale del numero di corse rispetto alle zone di partenza
 * (la zona di partenza è indicata da PULocationID), la mancia media e la sua deviazione standard, il
 * metodo di pagamento più diffuso.
 * Esempio di output:
 * # header: YYYY-MM-DD-HH, perc PU1, perc PU2, ... perc PU265, avg tip, stddev tip, pref payment
 * 12022-01-01-00, 0.21, 0, ..., 0.24, 18.3, 6.31, 1
 */

//TODO modificato tutto per avere orario in stringa secondo il nuovo formato. Da rifare tutta ma penso servirà

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
import utils.map.Payment;
import utils.Tools;
import utils.valq.ValQ2;

import java.util.List;

import static utils.Tools.*;

public class Query2 extends Query{
    public Query2(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection, String name) {
        super(spark, dataset, collection, name);
    }

    public long execute() {

        // RDD:=[time_slot,statistics]
        JavaPairRDD<String, ValQ2> aggregated = dataset.mapToPair(r ->
                    new Tuple2<>(Tools.getHour(r.getTimestamp(0)),
                    new ValQ2(r.getDouble(4), r.getLong(2),1)));


        /**
         * Calcolo del metodo di pagamento più diffuso per ogni fascia oraria
         */

        // RDD:=[(time_slot,payment),1]
        JavaPairRDD<Tuple2<String, Long>,Integer> aggr_pay = aggregated.mapToPair(r ->
                new Tuple2<>(
                        new Tuple2 <>(
                                r._1(),
                                r._2().getPayment_type())
                ,1));

        // RDD:=[(time_slot,payment),occurrences]
        JavaPairRDD<Tuple2<String, Long>,Integer> red_pay = aggr_pay.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> {
            Integer occ = v1+ v2;
            return occ;
        });

        // RDD:=[time_slot,{((time_slot,payment),occurrences)...}]
        JavaPairRDD<String, Iterable<Tuple2<Tuple2<String, Long>, Integer>>> grouped = red_pay.groupBy((Function<Tuple2<Tuple2<String, Long>, Integer>, String>) r -> r._1()._1());

        // RDD:=[time_slot,(top_payment,occurrences)]
        JavaPairRDD<String,Tuple2<Long,Integer>> top_payments = grouped.mapToPair(r ->
                new Tuple2<>(
                        r._1(),
                        getMostFrequentPayment(r._2())
                ));

        /**
         * Calcolo della media e deviazione standard di 'tips' per ogni fascia oraria
         */
        // RDD:=[time_slot,statistics_occurrences]
        JavaPairRDD<String, ValQ2> reduced = aggregated.reduceByKey((Function2<ValQ2, ValQ2, ValQ2>) (v1, v2) -> {
            Double tips = v1.getTips() + v2.getTips();
            Integer occ = v1.getOccurrences() + v2.getOccurrences();
            ValQ2 v = new ValQ2(tips, occ);
            return v;
        });

        // RDD:=[time_slot,statistics_mean]
        JavaPairRDD<String, ValQ2> statistics = reduced.mapToPair(
                r -> {
                    Integer num_occurrences = r._2().getOccurrences();
                    Double tips_mean = r._2().getTips() / num_occurrences;

                    return new Tuple2<>(r._1(),
                            new ValQ2(tips_mean, num_occurrences));
                });

        JavaPairRDD<String, Tuple2<ValQ2, ValQ2>> joined = aggregated.join(statistics);

        // RDD:=[time_slot,statistics_deviation_it]
        JavaPairRDD<String, ValQ2> iterations = joined.mapToPair(
                r -> {
                    Double tips_mean = r._2()._2().getTips();
                    Double tip_val = r._2()._1().getTips();
                    Double tips_dev = Math.pow((tip_val - tips_mean), 2);
                    r._2()._2().setTips_stddev(tips_dev);

                    return new Tuple2<>(r._1(), r._2()._2());
                });

        // RDD:=[time_slot,statistics_deviation_sum]
        JavaPairRDD<String, ValQ2> stddev_aggr = iterations.reduceByKey((Function2<ValQ2, ValQ2, ValQ2>) (v1, v2) -> {
            Double tips_total_stddev = v1.getTips_stddev() + v2.getTips_stddev();
            ValQ2 v = new ValQ2(v1.getTips(), v1.getOccurrences(), v1.getPayment_type(), tips_total_stddev);
            return v;
        });

        // RDD:=[time_slot,statistics_deviation]
        JavaPairRDD<String, ValQ2> deviation = stddev_aggr.mapToPair(
                r -> {
                    Double tips_mean = r._2().getTips();
                    Integer n = r._2().getOccurrences();
                    Double tips_dev = Math.sqrt(r._2().getTips_stddev() / n);
                    ValQ2 v = new ValQ2(tips_mean, n, tips_dev);
                    return new Tuple2<>(r._1(), v);
                });

        /**
         * Unione dei metodi di pagamento con le statistiche calcolate
         */
        // RDD:=[time_slot,(statistics,(top_payment,occurrences)))]
        JavaPairRDD<String, Tuple2<ValQ2, Tuple2<Long, Integer>>> final_joined = deviation
                .join(top_payments)
                .sortByKey();

        List<Tuple2<String, Tuple2<ValQ2, Tuple2<Long, Integer>>>> result = final_joined.collect();

        /**
         * Salvataggio dei risultati su mongodb
         */
        for (Tuple2<String, Tuple2<ValQ2, Tuple2<Long, Integer>>> r:result) {
            String slot = r._1();
            Double mean = r._2()._1().getTips();
            Double stdev = r._2()._1().getTips_stddev();
            Integer payment_id = Math.toIntExact(r._2()._2()._1());
            String payment_name = Payment.staticMap.get(payment_id);
            Integer payment_occ = r._2()._2()._2();

            Document document = new Document();
            document.append("hour_slot", slot);
            document.append("tips_mean", mean);
            document.append("tips_stdev", stdev);
            document.append("top_payment", payment_id);
            document.append("payment_name", payment_name);
            document.append("payment_occ", payment_occ);

            collection.insertOne(document);
        }
        return 0;
    }
}
