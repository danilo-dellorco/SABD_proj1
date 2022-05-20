/**
 *  Average calculation on a monthly basis and on a
 *  subset of values tip/(total amount - toll amount)
 */

package queries;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.TaxiRow;
import java.util.ArrayList;
import java.util.List;
import static utils.Tools.ParseRow;


public class Query1 extends Query {
    private static List query1_results = new ArrayList();

    public Query1(SparkSession spark, JavaRDD<Row> dataset) {
        super(spark, dataset);
    }

    @Override
    public void execute() {
        JavaRDD<TaxiRow> taxiRows = dataset.map(r -> ParseRow(r));

        // Analisys of the single Month
        JavaRDD<Double> tips = taxiRows.map(t -> t.getTip_amount());
        JavaRDD<Double> total = taxiRows.map(t -> t.getTotal_amount());
        JavaRDD<Double> tolls = taxiRows.map(t -> t.getTolls_amount());

        double total_tips = tips.reduce((a, b) -> a + b);
        double total_amount = total.reduce((a, b) -> a + b);
        double tolls_amount = tolls.reduce((a, b) -> a + b);

        double mean = total_tips / (total_amount - tolls_amount);

        query1_results.add(mean);
    }

    @Override
    public void print() {

    }
}
