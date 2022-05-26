import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import queries.*;
import queriesSQL.Query1SQL;
import queriesSQL.Query2SQL;
import utils.Config;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static utils.Tools.*;

public class Main {
    public static SparkSession spark = null;
    public static JavaRDD<Row> yellowRDD = null;
    public static JavaRDD<Row> greenRDD = null;
    public static List<MongoCollection> collections = null;
    public static Query query;

    public static String jar_path;
    public static String yellow_dataset_path;
    public static String green_dataset_path;
    public static String spark_url;


    //TODO vedere il caching per gli RDD riacceduti
    public static void main(String[] args) {
        setExecMode();
        initSpark();
        loadDataset();
        initMongo();
        turnOffLogger();

        Query1 q1 = new Query1(spark, yellowRDD,collections.get(0), "QUERY 3");
        Query2 q2 = new Query2(spark, yellowRDD,collections.get(1), "QUERY 4");
        Query3 q3 = new Query3(spark, yellowRDD,collections.get(2), "QUERY 1 SQL");
        Query4 q4 = new Query4(spark, greenRDD,collections.get(3), "QUERY 2 SQL");

        Query1SQL q1SQL = new Query1SQL(spark, yellowRDD,collections.get(3), "QUERY 1");
        Query2SQL q2SQL = new Query2SQL(spark, yellowRDD,collections.get(4), "QUERY 2");

        switch (args[0]) {
            case ("Q1"):
                query = q1;
                break;
            case ("Q2"):
                query=q2;
                break;
            case ("Q3"):
                query=q3;
                break;
            case ("Q4"):
                query=q4;
                break;
            case ("Q1SQL"):
                query=q1SQL;
                break;
            case ("Q2SQL"):
                query=q2SQL;
                break;
        }

        printSystemSpecs();
        Timestamp start = getTimestamp();
        query.execute();
        Timestamp end = getTimestamp();
        long exec = end.getTime() - start.getTime();
        query.printResults();
        System.out.println(String.format("%s execution time: %s", query.getName(), toMinutes(exec)));
        promptEnterKey();
    }

    /**
     * Inizializza il client mongodb per scrivere i risultati delle query.
     * @return lista di collezioni su cui ogni query scrive il proprio output
     */
    public static void initMongo() {
        MongoClient mongo = new MongoClient(new MongoClientURI(Config.MONGO_URL)); //add mongo-server to /etc/hosts
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
        boolean collExists4 = db.listCollectionNames().into(new ArrayList<>()).contains(Config.MONGO_Q1SQL);
        if (collExists4) {
            db.getCollection(Config.MONGO_Q1SQL).drop();
        }
        boolean collExists5 = db.listCollectionNames().into(new ArrayList<>()).contains(Config.MONGO_Q2SQL);
        if (collExists5) {
            db.getCollection(Config.MONGO_Q2SQL).drop();
        }

        db.createCollection(Config.MONGO_Q1);
        db.createCollection(Config.MONGO_Q1SQL);
        db.createCollection(Config.MONGO_Q2);
        db.createCollection(Config.MONGO_Q2SQL);
        db.createCollection(Config.MONGO_Q3);

        MongoCollection collection1 = db.getCollection(Config.MONGO_Q1);
        MongoCollection collection2 = db.getCollection(Config.MONGO_Q2);
        MongoCollection collection3 = db.getCollection(Config.MONGO_Q3);
        MongoCollection collection1_SQL = db.getCollection(Config.MONGO_Q1SQL);
        MongoCollection collection2_SQL = db.getCollection(Config.MONGO_Q2SQL);

        collections = Arrays.asList(collection1,collection2,collection3, collection1_SQL, collection2_SQL);
    }

    /**
     * Inizializza Spark ritornando la spark session
     * @return
     */
    public static void initSpark() {
        SparkConf conf = new SparkConf().setJars(new String[]{jar_path});
        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .master(spark_url)
                .appName("SABD Proj 1")
                .getOrCreate();
        spark=sparkSession;
    }

    /**
     * Carica il dataset nel sistema, convertendolo da Parquet in un RDD
     * @return
     */
    public static void loadDataset() {
        JavaRDD<Row> rows;
        if (Config.DATA_MODE.equals("UNLIMITED")) {
            rows = spark.read().option("header", "false").parquet(yellow_dataset_path).toJavaRDD();
        }
        else {
            rows = spark.read().option("header", "false").parquet(yellow_dataset_path).limit(Config.LIMIT_NUM).toJavaRDD();
        }
        yellowRDD = rows;
        greenRDD = spark.read().option("header", "false").parquet(green_dataset_path).toJavaRDD();
    }

    /**
     * Configura i path per l'esecuzione locale oppure su container docker.
     */
    public static void setExecMode() {
        if (Config.EXEC_MODE.equals("LOCAL")) {
            jar_path = Config.LOCAL_JAR_PATH;
            yellow_dataset_path = Config.LOCAL_YELLOW_DATASET_PATH;
            green_dataset_path = Config.LOCAL_GREEN_DATASET_PATH;
            spark_url = Config.LOCAL_SPARK_URL;
        } else {
            jar_path = Config.JAR_PATH;
            yellow_dataset_path = Config.YELLOW_DATASET_PATH;
            green_dataset_path = Config.GREEN_DATASET_PATH;
            spark_url = Config.SPARK_URL;
        }
    }

    /**
     * Disattiva i messaggi di logging
     */
    public static void turnOffLogger() {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }
}