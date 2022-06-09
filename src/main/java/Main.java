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
import utils.Tools;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
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

    // TODO mettere nella relazione la pulizia del dataset insomma righe e colonne tolte e perché.
    // TODO vedere il caching per gli RDD riacceduti
    // TODO rimuovere i sortbykey intermedi perchè sono wide transformation. Non dovrebbero avere utilità pratiche ma li usavamo solo per i print intermedi (sopratutto query2)
    // TODO vedere i DAG delle query e togliere cose inutili
    public static void main(String[] args) throws IOException, InterruptedException {
        setExecMode();
        long sparkSetupTime = initSpark();
        long dataLoadTime = loadDataset();
        long mongoSetupTime = initMongo();
        turnOffLogger();

        Query1 q1 = new Query1(spark, yellowRDD,collections.get(0), "QUERY 1");
        Query2 q2 = new Query2(spark, yellowRDD,collections.get(1), "QUERY 2");
        Query3 q3 = new Query3(spark, yellowRDD,collections.get(2), "QUERY 3");
        Query4 q4 = new Query4(spark, greenRDD,collections.get(3), "QUERY 4");

        Query1SQL q1SQL = new Query1SQL(spark, yellowRDD,collections.get(4), "QUERY 1 SQL");
        Query2SQL q2SQL = new Query2SQL(spark, yellowRDD,collections.get(5), "QUERY 2 SQL");

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
        long queryExecTime = query.execute();
        long mongoSaveTime = query.writeResultsOnMongo();
        long csvSaveTime = query.writeResultsOnCSV();

//        query.printResults();
        printResultAnalysis(query.getName(),sparkSetupTime, dataLoadTime, mongoSetupTime, queryExecTime,mongoSaveTime,csvSaveTime);
        promptEnterKey();
    }

    /**
     * Inizializza il client mongodb per scrivere i risultati delle query.
     * @return lista di collezioni su cui ogni query scrive il proprio output
     */
    public static long initMongo() {
        Timestamp start = getTimestamp();

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
        if (db.listCollectionNames().into(new ArrayList<>()).contains(Config.MONGO_Q4)) {
               db.getCollection(Config.MONGO_Q4).drop();
        }
        if (db.listCollectionNames().into(new ArrayList<>()).contains(Config.MONGO_Q1SQL)) {
            db.getCollection(Config.MONGO_Q1SQL).drop();
        }
        if (db.listCollectionNames().into(new ArrayList<>()).contains(Config.MONGO_Q2SQL)) {
            db.getCollection(Config.MONGO_Q2SQL).drop();
        }

        db.createCollection(Config.MONGO_Q1);
        db.createCollection(Config.MONGO_Q2);
        db.createCollection(Config.MONGO_Q3);
        db.createCollection(Config.MONGO_Q4);
        db.createCollection(Config.MONGO_Q1SQL);
        db.createCollection(Config.MONGO_Q2SQL);

        MongoCollection collection1 = db.getCollection(Config.MONGO_Q1);
        MongoCollection collection2 = db.getCollection(Config.MONGO_Q2);
        MongoCollection collection3 = db.getCollection(Config.MONGO_Q3);
        MongoCollection collection4 = db.getCollection(Config.MONGO_Q4);
        MongoCollection collection1_SQL = db.getCollection(Config.MONGO_Q1SQL);
        MongoCollection collection2_SQL = db.getCollection(Config.MONGO_Q2SQL);

        collections = Arrays.asList(collection1,collection2,collection3,collection4, collection1_SQL, collection2_SQL);
        Timestamp end = getTimestamp();
        return end.getTime()-start.getTime();
    }

    /**
     * Inizializza Spark ritornando la spark session
     * @return
     */
    public static long initSpark() {
        Timestamp start = getTimestamp();
        SparkConf conf = new SparkConf().setJars(new String[]{jar_path});
        spark = SparkSession
                .builder()
                .config(conf)
                .master(spark_url)
                .appName("SABD Proj 1")
                .getOrCreate();
        Timestamp end = getTimestamp();
        return end.getTime()-start.getTime();
    }

    /**
     * Carica il dataset nel sistema, convertendolo da Parquet in un RDD
     * @return
     */
    public static long loadDataset() {
        Timestamp start = getTimestamp();
        if (Config.DATA_MODE.equals("UNLIMITED")) {
            yellowRDD = spark.read().option("header", "false").parquet(yellow_dataset_path).toJavaRDD();
            greenRDD = spark.read().option("header", "false").parquet(green_dataset_path).toJavaRDD();
        }
        else {
            yellowRDD = spark.read().option("header", "false").parquet(yellow_dataset_path).limit(Config.LIMIT_NUM).toJavaRDD();
            greenRDD = spark.read().option("header", "false").parquet(green_dataset_path).limit(Config.LIMIT_NUM).toJavaRDD();
        }
        Timestamp end = getTimestamp();
        return end.getTime()-start.getTime();
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