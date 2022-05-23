/**
 * Average calculation on a monthly basis and on a
 * subset of values tip/(total amount - toll amount)
 */

package queries;

import com.mongodb.client.MongoCollection;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import utils.Month;
import utils.TaxiRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static utils.Tools.ParseRow;
//TODO Ci stanno dei dati con mesi diversi da Dicembre-Gennaio-Febbraio

public class Query1 extends Query {

    public Query1(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection) {
        super(spark, dataset, collection);
    }

    @Override
    public void execute() {

        JavaPairRDD<Integer, TaxiRow> taxiRows = dataset.mapToPair(
                r -> new Tuple2<>(r.getTimestamp(0).getMonth(),
                        ParseRow(r)));

//        taxiRows.foreach((VoidFunction<Tuple2<Integer, TaxiRow>>) r -> System.out.println(r));

        // TODO valutare creazione classe ValQ1, perchè usando TaxiRow dovremmo settare anche tutti gli altri campi che sono inutili per la query
        JavaPairRDD<Integer, TaxiRow> reduced = taxiRows.reduceByKey((Function2<TaxiRow, TaxiRow, TaxiRow>) (v1, v2) -> {
            Double tips = v1.getTip_amount() + v2.getTip_amount();
            Double total = v1.getTotal_amount() + v2.getTotal_amount();
            Double tolls = v1.getTolls_amount() + v2.getTolls_amount();

            TaxiRow v = new TaxiRow();
            v.setTip_amount(tips);
            v.setTotal_amount(total);
            v.setTolls_amount(tolls);
            return v;
        });

        Map<Integer, Long> counted = taxiRows.countByKey();
        counted.forEach((integer, aLong) -> System.out.println("Month: " + integer + " Counted: " + aLong));


//        reduced.foreach((VoidFunction<Tuple2<Integer, TaxiRow>>) r -> System.out.println(String.format("Month: %d Values: {Total amount: %,.020f | Total tips: %,.020f |" +
//                "Total tolls: %,.020f}", r._1(), r._2().getTotal_amount(), r._2().getTip_amount(), r._2().getTolls_amount())));
//        reduced.foreach((VoidFunction<Tuple2<Integer, TaxiRow>>) r -> System.out.println("Month: " + r._1() + " Values: {Total amount: " + r._2().getTotal_amount() + " | Total tips: " + r._2().getTip_amount() +
//                " | Total tolls: " + r._2().getTolls_amount() + " }"));
        JavaPairRDD<Integer, Double> results = reduced.mapToPair(
                r -> {
                    Double tips = r._2().getTip_amount();
                    Double tolls = r._2().getTolls_amount();
                    Double total = r._2().getTotal_amount();
                    Double mean = tips / (total - tolls);
                    return new Tuple2<>(r._1(), mean);
                }
        );
        //TODO Valutare cosa salvare su HDFS considerando che i risultati potrebbero essere riutilizzati per altre query
        results.foreach((VoidFunction<Tuple2<Integer, Double>>) r -> System.out.println(String.format("Month: %d-%s Mean: %,.010f", r._1(), Month.staticMap.get(r._1()), r._2())));
    }

    @Override
    public void print() {

    }
}
