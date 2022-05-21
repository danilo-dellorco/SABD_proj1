import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jvnet.hk2.annotations.Optional;
import queries.*;
import utils.Config;
import utils.TaxiRow;

import static utils.Tools.ParseRow;
import static utils.Tools.promptEnterKey;

public class Main {
    public static final SparkSession spark = initSpark();
//    public static final JavaRDD<Row> datasetRDD = loadDataset(1000);
    public static final JavaRDD<Row> datasetRDD = loadDataset();

    //TODO vedere il caching per gli RDD riacceduti
    //TODO scrivere gli output su file HDFS
    public static void main(String[] args) {
        turnOffLogger();

        Query1 q1 = new Query1(spark, datasetRDD);
        Query2 q2 = new Query2(spark, datasetRDD);
        Query3 q3 = new Query3(spark, datasetRDD);

//        q1.execute();
        q2.execute();
//        q3.execute();
        promptEnterKey();
    }

    public static SparkSession initSpark() {
        SparkSession spark = SparkSession
                .builder()
                .master(Config.SPARK_URL)
                .appName("SABD Proj 1")
                .getOrCreate();
        return spark;
    }

    public static JavaRDD<Row> loadDataset(int limit) {
        JavaRDD<Row> rows1 = spark.read().option("header", "false").parquet(Config.DAT1_PATH).limit(limit).toJavaRDD();
        JavaRDD<Row> rows2 = spark.read().option("header", "false").parquet(Config.DAT2_PATH).limit(limit).toJavaRDD();
        JavaRDD<Row> rows3 = spark.read().option("header", "false").parquet(Config.DAT3_PATH).limit(limit).toJavaRDD();
        JavaRDD<Row> merged = rows1.union(rows2).union(rows3);
        return merged;
    }

    public static JavaRDD<Row> loadDataset() {
        JavaRDD<Row> rows1 = spark.read().option("header", "false").parquet(Config.DAT1_PATH).toJavaRDD();
        JavaRDD<Row> rows2 = spark.read().option("header", "false").parquet(Config.DAT2_PATH).toJavaRDD();
        JavaRDD<Row> rows3 = spark.read().option("header", "false").parquet(Config.DAT3_PATH).toJavaRDD();
        JavaRDD<Row> merged = rows1.union(rows2).union(rows3);
        return merged;
    }



    public static void turnOffLogger() {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }
}