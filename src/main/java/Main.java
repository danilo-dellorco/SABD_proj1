import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
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
    public static SparkSession spark = null;
    public static JavaRDD<Row> datasetRDD = null;
    public static List<MongoCollection> collections = null;

    public static String jar_path;
    public static String dataset_path;
    public static String spark_url;

    public static final String MODE = "LOCAL";
    //TODO vedere il caching per gli RDD riacceduti
    //TODO scrivere gli output su file HDFS
    public static void main(String[] args) {

        if (MODE.equals("LOCAL")) {
            jar_path = Config.LOCAL_JAR_PATH;
            dataset_path = Config.LOCAL_DAT1_PATH;
            spark_url = Config.LOCAL_SPARK_URL;
        } else {
            jar_path = Config.JAR_PATH;
            dataset_path = Config.DAT1_PATH;
            spark_url = Config.SPARK_URL;
        }

        spark = initSpark();
        datasetRDD = loadDataset();
        collections = initMongo();

        turnOffLogger();

        Query1SQL q1SQL = new Query1SQL(spark, datasetRDD,collections.get(0));
        Query2SQL q2SQL = new Query2SQL(spark, datasetRDD,collections.get(1));
//        Query2SQL q3SQL = new Query3SQL(spark, datasetRDD,collections.get(2));
//          q1SQL.execute();
//          q2SQL.execute();
        Query1 q1 = new Query1(spark, datasetRDD,collections.get(0));
        Query2 q2 = new Query2(spark, datasetRDD,collections.get(1));
        Query3 q3 = new Query3(spark, datasetRDD,collections.get(2));

//        q1.execute();
        q2.execute();
//        q3.execute();
        promptEnterKey();
    }

    public static List<MongoCollection> initMongo() {
        MongoClient mongo = new MongoClient(new MongoClientURI("mongodb://mongo-server:27017"));
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
        SparkConf conf = new SparkConf().setJars(new String[]{jar_path});
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .master(spark_url)
                .appName("SABD Proj 1")
                .getOrCreate();
        return spark;
    }

    public static JavaRDD<Row> loadDataset() {

//        JavaRDD<Row> rows1 = spark.read().option("header", "false").parquet(Config.DAT1_PATH).toJavaRDD();
//        JavaRDD<Row> rows2 = spark.read().option("header", "false").parquet(Config.DAT2_PATH).toJavaRDD();
//        JavaRDD<Row> rows3 = spark.read().option("header", "false").parquet(Config.DAT3_PATH).toJavaRDD();
        JavaRDD<Row> rows3 = spark.read().option("header", "false").parquet(dataset_path).limit(100).toJavaRDD();
//        JavaRDD<Row> merged = rows1.union(rows2).union(rows3);
//        return merged;
        return rows3;
    }



    public static void turnOffLogger() {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }
}