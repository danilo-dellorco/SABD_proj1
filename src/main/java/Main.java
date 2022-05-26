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
import sparkSQL.*;
import utils.Config;
import utils.Tools;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static utils.Tools.promptEnterKey;

public class Main {
    public static SparkSession spark = null;
    public static JavaRDD<Row> yellowRDD = null;
    public static JavaRDD<Row> greenRDD = null;
    public static List<MongoCollection> collections = null;

    public static String jar_path;
    public static String yellow_dataset_path;
    public static String green_dataset_path;
    public static String spark_url;

    // EXEC_MODE = {LOCAL,DOCKER}
    public static final String EXEC_MODE = "LOCAL";
    // DATA_MODE = {LIMITED,UNLIMITED}
    private static final String DATA_MODE = "UNLIMITED";
    private static final int LIMIT_NUM = 1000;

    //TODO vedere il caching per gli RDD riacceduti
    public static void main(String[] args) {
        setExecMode();
        initSpark();
        loadDataset();
        initMongo();
        turnOffLogger();

//        Query3 q4 = new Query4(spark, datasetRDD,collections.get(3));

        Timestamp start=null, end = null;
        System.out.println("----------- "+args[0]);
        switch (args[0]){
            case "Q1":
                System.out.println("executing Q1");
                Query1 q1 = new Query1(spark, yellowRDD,collections.get(0));
                start = Tools.getTimestamp();
                q1.execute();
                end = Tools.getTimestamp();
                System.out.println("finished Q1");
                break;
            case "Q2":
                Query2 q2 = new Query2(spark, yellowRDD,collections.get(1));
                start = Tools.getTimestamp();
                q2.execute();
                end = Tools.getTimestamp();
                break;
            case "Q3":
                Query3 q3 = new Query3(spark, yellowRDD,collections.get(2));
                start = Tools.getTimestamp();
                q3.execute();
                end = Tools.getTimestamp();
                break;
            case "Q1SQL":
                Query1SQL q1SQL = new Query1SQL(spark, yellowRDD,collections.get(3));
                start = Tools.getTimestamp();
                q1SQL.execute();
                end = Tools.getTimestamp();
                break;
            case "Q2SQL":
                Query2SQL q2SQL = new Query2SQL(spark, yellowRDD,collections.get(4));
                start = Tools.getTimestamp();
                q2SQL.execute();
                end = Tools.getTimestamp();
                break;
        }

        System.out.println(args[0]+" execution time: "+Tools.toMinutes(end.getTime()-start.getTime()));
//        promptEnterKey();
    }

    /**
     * Inizializza il client mongodb per scrivere i risultati delle query.
     * @return lista di collezioni su cui ogni query scrive il proprio output
     */
    public static void initMongo() {
        MongoClient mongo = new MongoClient(new MongoClientURI(Config.MONGO_URL)); //add mongo-server to /etc/hosts
        MongoDatabase db = mongo.getDatabase(Config.MONGO_DB);
        if (db.listCollectionNames().into(new ArrayList<>()).contains(Config.MONGO_Q1)) {
            db.getCollection(Config.MONGO_Q1).drop();
        }
        if (db.listCollectionNames().into(new ArrayList<>()).contains(Config.MONGO_Q2)) {
            db.getCollection(Config.MONGO_Q2).drop();
        }
        if (db.listCollectionNames().into(new ArrayList<>()).contains(Config.MONGO_Q3)) {
            db.getCollection(Config.MONGO_Q3).drop();
        }
        if (db.listCollectionNames().into(new ArrayList<>()).contains(Config.MONGO_Q1SQL)) {
            db.getCollection(Config.MONGO_Q1SQL).drop();
        }
        if (db.listCollectionNames().into(new ArrayList<>()).contains(Config.MONGO_Q2SQL)) {
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
        spark = SparkSession
                .builder()
                .config(conf)
                .master(spark_url)
                .appName("SABD Proj 1")
                .getOrCreate();
    }

    /**
     * Carica il dataset nel sistema, convertendolo da Parquet in un RDD
     * @return
     */
    public static void loadDataset() {
        JavaRDD<Row> rows;
        if (DATA_MODE.equals("UNLIMITED")) {
            yellowRDD = spark.read().option("header", "false").parquet(yellow_dataset_path).toJavaRDD();
        }
        else {
            yellowRDD = spark.read().option("header", "false").parquet(yellow_dataset_path).limit(LIMIT_NUM).toJavaRDD();
        }
        greenRDD = spark.read().option("header", "false").parquet(green_dataset_path).toJavaRDD();
    }

    /**
     * Configura i path per l'esecuzione locale oppure su container docker.
     */
    public static void setExecMode() {
        if (EXEC_MODE.equals("LOCAL")) {
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