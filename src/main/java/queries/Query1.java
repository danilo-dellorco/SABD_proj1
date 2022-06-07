/**
 * Average calculation on a monthly basis and on a
 * subset of values tip/(total amount - toll amount)
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
import utils.ValQ1;
import utils.Tools;

import java.io.FileWriter;
import java.util.List;

@SuppressWarnings("ALL")
public class Query1 extends Query {
    List<Tuple2<String, Tuple2<Double, Integer>>> results;

    public Query1(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection, String name) {
        super(spark, dataset, collection, name);
    }

    @Override
    public void execute() {
        // RDD:=[month,values]
        JavaPairRDD<String, ValQ1> taxiRows = dataset.mapToPair(
                r -> {
                    String month = Tools.getMonth(r.getTimestamp(0));
                    ValQ1 v1 = new ValQ1(r.getDouble(4),r.getDouble(6),r.getDouble(5), r.getLong(2),1);
                    return new Tuple2<>(month, v1);
                });

        // Mantengo solo gli RDD con metodo di pagamento "credit card"
        JavaPairRDD<String, ValQ1> filtered = taxiRows.filter((Function<Tuple2<String, ValQ1>, Boolean>) r -> r._2().getPayment_type()==1);

        // RDD:=[month,values_aggr]
        JavaPairRDD<String, ValQ1> reduced = filtered.reduceByKey((Function2<ValQ1, ValQ1, ValQ1>) (v1, v2) -> {
            Double tips = v1.getTip_amount() + v2.getTip_amount();
            Double total = v1.getTotal_amount() + v2.getTotal_amount();
            Double tolls = v1.getTolls_amount() + v2.getTolls_amount();
            Integer trips = v1.getTrips_number() + v2.getTrips_number();

            ValQ1 v = new ValQ1();
            v.setTip_amount(tips);
            v.setTotal_amount(total);
            v.setTolls_amount(tolls);
            v.setTrips_number(trips);
            return v;
        });

        // result_list:=[month,tip_percentage,trips_number]
        results = reduced.mapToPair(
                r -> {
                    Double tips = r._2().getTip_amount();
                    Double tolls = r._2().getTolls_amount();
                    Double total = r._2().getTotal_amount();
                    Double mean = tips / (total - tolls);
                    Integer trips = r._2().getTrips_number();
                    return new Tuple2<>(r._1(), new Tuple2<>(mean, trips));
                }
        ).sortByKey().collect();

        writeResultsOnMongo();
        writeResultsOnCSV();
    }

    @Override
    public void writeResultsOnMongo() {

        for (Tuple2<String, Tuple2<Double,Integer>> r : results) {
            String month = r._1();
            Double percentage = r._2()._1();
            Integer trips = r._2()._2();

            Document document = new Document();
            document.append("month_id", month);
            document.append("tip_percentage", percentage);
            document.append("trips_number", trips);

            collection.insertOne(document);
        }
    }

    // TODO finire e testare
    @Override
    public void writeResultsOnCSV() {
        String outputName = "q1-res.csv";

        try (FileWriter fileWriter = new FileWriter(outputName)) {
            StringBuilder outputBuilder = new StringBuilder("YYYY-MM;tip percentage;trips number\n");
            for (Tuple2<String, Tuple2<Double,Integer>> r : results) {
                String month = r._1();
                Double percentage = r._2()._1();
                Integer trips = r._2()._2();
                outputBuilder.append( month + ";" + percentage + ";"+ trips + "\n");
            }
            fileWriter.append(outputBuilder.toString());

        } catch (Exception e) {
            System.out.println("Results CSV Error: " + e.toString());
        }


    }
}
