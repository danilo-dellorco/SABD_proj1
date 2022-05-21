package queries;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class Query {
    public SparkSession spark;
    public JavaRDD<Row> dataset;

    public Query(SparkSession spark, JavaRDD<Row> dataset) {
        this.spark = spark;
        this.dataset = dataset;
    }

    public abstract void execute();
    public abstract void print();

}
