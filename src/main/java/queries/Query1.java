/**
 *  Average calculation on a monthly basis and on a
 *  subset of values tip/(total amount - toll amount)
 */

package queries;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
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

        JavaPairRDD<Integer, TaxiRow> taxiRows = dataset.mapToPair(
                r -> new Tuple2<>(r.getTimestamp(1).getMonth(),
                        ParseRow(r)));

        JavaPairRDD<Integer, TaxiRow> reduced = taxiRows.foldByKey(new TaxiRow(), (Function2<TaxiRow, TaxiRow, TaxiRow>) (v1, v2) -> {
            Double tips = v1.getTip_amount() + v2.getTip_amount();
            Double total = v1.getTotal_amount() + v2.getTotal_amount();
            Double tolls = v1.getTolls_amount() + v2.getTolls_amount();
            TaxiRow v = new TaxiRow();
            v.setTip_amount(tips);
            v.setTotal_amount(total);
            v.setTolls_amount(tolls);
            return v;
        });
d    }

    @Override
    public void print() {

    }
}
