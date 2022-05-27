/**
 * Average calculation on a monthly basis and on a
 * subset of values tip/(total amount - toll amount)
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
import utils.Month;
import utils.ValQ1;
import utils.Tools;

import java.util.List;

@SuppressWarnings("ALL")
public class Query1 extends Query {

    public Query1(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection, String name) {
        super(spark, dataset, collection, name);
    }

    @Override
    public void execute() {
        // RDD:=[month,values]
        JavaPairRDD<Integer, ValQ1> taxiRows = dataset.mapToPair(
                r -> {
                    Integer ts = Tools.getMonth(r.getTimestamp(0));
                    ValQ1 v1 = new ValQ1(r.getDouble(4),r.getDouble(6),r.getDouble(5));
                    return new Tuple2<>(ts, v1);
                });

        // RDD:=[month,values_aggr]
        JavaPairRDD<Integer, ValQ1> reduced = taxiRows.reduceByKey((Function2<ValQ1, ValQ1, ValQ1>) (v1, v2) -> {
            Double tips = v1.getTip_amount() + v2.getTip_amount();
            Double total = v1.getTotal_amount() + v2.getTotal_amount();
            Double tolls = v1.getTolls_amount() + v2.getTolls_amount();

            ValQ1 v = new ValQ1();
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
    }
}
