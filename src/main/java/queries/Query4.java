/**
 *     Identify the top-5 most popular DOLocationIDs (TLC
 *     Taxi Destination Zones), indicating for each one the
 *     average number of passengers and the mean and
 *     standard deviation of Fare_amount
 */

package queries;

import com.mongodb.client.MongoCollection;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Query4 extends Query{

    public Query4(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection, String name) {
        super(spark, dataset, collection, name);
    }


    @Override
    public void execute() {}
}
