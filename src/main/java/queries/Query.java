package queries;

import com.mongodb.client.MongoCollection;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class Query {
    public SparkSession spark;
    public JavaRDD<Row> dataset;
    public MongoCollection collection;

    public Query(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection) {
        this.spark = spark;
        this.dataset = dataset;
        this.collection = collection;
    }

    public abstract void execute();
    public abstract void print();

}
