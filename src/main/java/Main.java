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
import sparkSQL.Query1SQL;
import utils.Config;
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

<<<<<<< HEAD
        Query1 q1 = new Query1(spark, datasetRDD);
        Query2 q2 = new Query2(spark, datasetRDD);
//        Query3 q3 = new Query3(spark, datasetRDD);
=======
        Query1SQL q1SQL = new Query1SQL(spark, yellowRDD,collections.get(0));
        Query2SQL q2SQL = new Query2SQL(spark, yellowRDD,collections.get(1));
        Query1 q1 = new Query1(spark, yellowRDD,collections.get(0));
        Query2 q2 = new Query2(spark, yellowRDD,collections.get(1));
        Query3 q3 = new Query3(spark, yellowRDD,collections.get(2));
//        Query3 q4 = new Query4(spark, datasetRDD,collections.get(3));
>>>>>>> 973f54a584e91c7f5390f7aeffa580f3a10285ab

        q1.execute();
        q2.execute();
        q3.execute();
//        q4.execute();

//        q1SQL.execute();
//        q2SQL.execute();
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
        db.createCollection(Config.MONGO_Q1);
        db.createCollection(Config.MONGO_Q2);
        db.createCollection(Config.MONGO_Q3);

        MongoCollection collection1 = db.getCollection(Config.MONGO_Q1);
        MongoCollection collection2 = db.getCollection(Config.MONGO_Q2);
        MongoCollection collection3 = db.getCollection(Config.MONGO_Q3);

        collections = Arrays.asList(collection1,collection2,collection3);
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
        if (DATA_MODE.equals("UNLIMITED")) {
            rows = spark.read().option("header", "false").parquet(yellow_dataset_path).toJavaRDD();
        }
        else {
            rows = spark.read().option("header", "false").parquet(yellow_dataset_path).limit(LIMIT_NUM).toJavaRDD();
        }
        yellowRDD = rows;
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