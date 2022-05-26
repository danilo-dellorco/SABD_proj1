/**
 * Average calculation on a monthly basis and on a
 * subset of values tip/(total amount - toll amount)
 */

package queries;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;
import utils.Month;
import utils.YellowTaxiRow;
import utils.Tools;

import java.util.List;

import static utils.Tools.ParseRow;
//TODO Ci stanno dei dati con mesi diversi da Dicembre-Gennaio-Febbraio

@SuppressWarnings("ALL")
public class Query1 extends Query {

    public Query1(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection) {
        super(spark, dataset, collection);
    }

    @Override
    public void execute() {

        // RDD:=[month,values]
        //TODO un ValQ1 invece di tutto Taxi Row
        JavaPairRDD<Integer, YellowTaxiRow> taxiRows = dataset.mapToPair(
                r -> {
                    YellowTaxiRow tr = ParseRow(r);
                    Integer ts = Tools.getMonth(tr.getTpep_dropoff_datetime());
                    return new Tuple2<>(ts, tr);
                });

        // RDD:=[month,values_aggr]
        // TODO valutare creazione classe ValQ1, perchè usando TaxiRow dovremmo settare anche tutti gli altri campi che sono inutili per la query
        JavaPairRDD<Integer, YellowTaxiRow> reduced = taxiRows.reduceByKey((Function2<YellowTaxiRow, YellowTaxiRow, YellowTaxiRow>) (v1, v2) -> {
            Double tips = v1.getTip_amount() + v2.getTip_amount();
            Double total = v1.getTotal_amount() + v2.getTotal_amount();
            Double tolls = v1.getTolls_amount() + v2.getTolls_amount();

            YellowTaxiRow v = new YellowTaxiRow();
            v.setTip_amount(tips);
            v.setTotal_amount(total);
            v.setTolls_amount(tolls);
            return v;
        });

        // result_list:=[month,mean_value]
        List<Tuple2<Integer, Double>> results = reduced.mapToPair(
                r -> {
                    Double tips = r._2().getTip_amount();
                    Double tolls = r._2().getTolls_amount();
                    Double total = r._2().getTotal_amount();
                    Double mean = tips / (total - tolls);
                    return new Tuple2<>(r._1(), mean);
                }
        ).sortByKey().collect();

        /**
         * Salvataggio dei risultati su mongodb
         */
        for (Tuple2<Integer, Double> r : results) {
            Integer monthId = r._1();
            String monthName = Month.staticMap.get(r._1());
            Double mean = r._2();

            Document document = new Document();
            document.append("month_id", monthId);
            document.append("month_name", monthName);
            document.append("mean", mean);

            collection.insertOne(document);

        }

        /**
         * Stampa a schermo dei risultati
         */
        System.out.println("\n—————————————————————————————————————————————————————————— QUERY 1 ——————————————————————————————————————————————————————————");
        FindIterable<Document> docs = collection.find();
        for (Document doc : docs) {
            System.out.println(doc);
        }
        System.out.println("—————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————\n");

    }
}
