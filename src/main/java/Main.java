import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.bson.conversions.Bson;
import queries.*;
import sparkSQL.Query1SQL;
import sparkSQL.Query2SQL;
import utils.Config;
import utils.TaxiRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static utils.Tools.ParseRow;
import static utils.Tools.promptEnterKey;

public class Main {
    public static final SparkSession spark = initSpark();
//    public static final JavaRDD<Row> datasetRDD = loadDataset(1000);
    public static final JavaRDD<Row> datasetRDD = loadDataset();
    public static final List<MongoCollection> collections = initMongo();

    //TODO vedere il caching per gli RDD riacceduti
    //TODO scrivere gli output su file HDFS
    public static void main(String[] args) {

        turnOffLogger();

//        Query1 q1 = new Query1(spark, datasetRDD);
//        Query2 q2 = new Query2(spark, datasetRDD);
//        Query3 q3 = new Query3(spark, datasetRDD);
//          Query1SQL q1SQL = new Query1SQL(spark, datasetRDD);
          Query2SQL q2SQL = new Query2SQL(spark, datasetRDD);
//          q1SQL.execute();
          q2SQL.execute();
        Query1 q1 = new Query1(spark, datasetRDD,collections.get(0));
        Query2 q2 = new Query2(spark, datasetRDD,collections.get(1));
        Query3 q3 = new Query3(spark, datasetRDD,collections.get(2));

//        q1.execute();
//        q2.execute();
//        q3.execute();
        promptEnterKey();
    }

    public static List<MongoCollection> initMongo() {
        MongoClient mongo = new MongoClient(Config.MONGO_URL, Config.MONGO_PORT);
        MongoDatabase db = mongo.getDatabase(Config.MONGO_DB);
        boolean collExists1 = db.listCollectionNames().into(new ArrayList<>()).contains(Config.MONGO_Q1);
        if (collExists1) {
            db.getCollection(Config.MONGO_Q1).drop();
        }
        boolean collExists2 = db.listCollectionNames().into(new ArrayList<>()).contains(Config.MONGO_Q2);
        if (collExists2) {
            db.getCollection(Config.MONGO_Q2).drop();
        }
        boolean collExists3 = db.listCollectionNames().into(new ArrayList<>()).contains(Config.MONGO_Q3);
        if (collExists3) {
            db.getCollection(Config.MONGO_Q3).drop();
        }
        db.createCollection(Config.MONGO_Q1);
        db.createCollection(Config.MONGO_Q2);
        db.createCollection(Config.MONGO_Q3);

        MongoCollection collection1 = db.getCollection(Config.MONGO_Q1);
        MongoCollection collection2 = db.getCollection(Config.MONGO_Q2);
        MongoCollection collection3 = db.getCollection(Config.MONGO_Q3);

        return Arrays.asList(collection1,collection2,collection3);
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
//        JavaRDD<Row> rows1 = spark.read().option("header", "false").parquet(Config.DAT1_PATH).limit(limit).toJavaRDD();
        JavaRDD<Row> rows1 = spark.read().option("header", "false").parquet("data/filtered-dec.parquet").limit(limit).toJavaRDD();
//        JavaRDD<Row> rows2 = spark.read().option("header", "false").parquet(Config.DAT2_PATH).limit(limit).toJavaRDD();
//        JavaRDD<Row> rows3 = spark.read().option("header", "false").parquet(Config.DAT3_PATH).limit(limit).toJavaRDD();
//        JavaRDD<Row> merged = rows1.union(rows2).union(rows3);
        return rows1;
    }

    public static JavaRDD<Row> loadDataset() {

//        JavaRDD<Row> rows1 = spark.read().option("header", "false").parquet(Config.DAT1_PATH).toJavaRDD();
//        JavaRDD<Row> rows2 = spark.read().option("header", "false").parquet(Config.DAT2_PATH).toJavaRDD();
//        JavaRDD<Row> rows3 = spark.read().option("header", "false").parquet(Config.DAT3_PATH).toJavaRDD();
        JavaRDD<Row> rows3 = spark.read().option("header", "false").parquet("data/filtered-dec.parquet").toJavaRDD();
//        JavaRDD<Row> merged = rows1.union(rows2).union(rows3);
//        return merged;
        return rows3;
    }



    public static void turnOffLogger() {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }
}