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
import queriesSQL.Query3SQL;
import utils.Config;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static utils.Tools.*;

public class Main {
    public static SparkSession spark = null;
    public static JavaRDD<Row> javaRDD = null;
    public static List<MongoCollection> collections = null;
    public static Query query;

    public static String jar_path;
    public static String dataset_path;
    public static String spark_url;


    //TODO vedere il caching per gli RDD riacceduti
    //TODO rimuovere i sortbykey intermedi perchè sono wide transformation. Non dovrebbero avere utilità pratiche ma li usavamo solo per i print intermedi (sopratutto query2)
    //TODO vedere i DAG delle query e togliere cose inutili
    //TODO cambiare le get delle colonne con indice sul dataset nelle query perchè è cambiato il dataset filtrato

    public static void main(String[] args) {
        setExecMode();
        initSpark();
        loadDataset();
        initMongo();
        turnOffLogger();

        Query1 q1 = new Query1(spark, javaRDD, collections.get(0), "QUERY 1");
        Query2 q2 = new Query2(spark, javaRDD, collections.get(1), "QUERY 2");
        Query3 q3 = new Query3(spark, javaRDD, collections.get(2), "QUERY 3");

        Query1SQL q1SQL = new Query1SQL(spark, javaRDD, collections.get(3), "QUERY 1 SQL");
        Query2SQL q2SQL = new Query2SQL(spark, javaRDD, collections.get(4), "QUERY 2 SQL");
        Query3SQL q3SQL = new Query3SQL(spark, javaRDD, collections.get(5), "QUERY 3 SQL");

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
            case ("Q1SQL"):
                query=q1SQL;
                break;
            case ("Q2SQL"):
                query=q2SQL;
                break;
            case ("Q3SQL"):
                query=q3SQL;
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
        if (db.listCollectionNames().into(new ArrayList<>()).contains(Config.MONGO_Q3SQL)) {
            db.getCollection(Config.MONGO_Q3SQL).drop();
        }

        db.createCollection(Config.MONGO_Q1);
        db.createCollection(Config.MONGO_Q2);
        db.createCollection(Config.MONGO_Q3);
        db.createCollection(Config.MONGO_Q1SQL);
        db.createCollection(Config.MONGO_Q2SQL);
        db.createCollection(Config.MONGO_Q3SQL);

        MongoCollection collection1 = db.getCollection(Config.MONGO_Q1);
        MongoCollection collection2 = db.getCollection(Config.MONGO_Q2);
        MongoCollection collection3 = db.getCollection(Config.MONGO_Q3);
        MongoCollection collection1_SQL = db.getCollection(Config.MONGO_Q1SQL);
        MongoCollection collection2_SQL = db.getCollection(Config.MONGO_Q2SQL);
        MongoCollection collection3_SQL = db.getCollection(Config.MONGO_Q3SQL);

        collections = Arrays.asList(collection1, collection2, collection3, collection1_SQL, collection2_SQL, collection3_SQL);
    }

    /**
     * Inizializza Spark ritornando la spark session
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
     */
    public static void loadDataset() {
        if (Config.DATA_MODE.equals("UNLIMITED")) {
            javaRDD = spark.read().option("header", "false").parquet(dataset_path).toJavaRDD();
        }
        else {
            javaRDD = spark.read().option("header", "false").parquet(dataset_path).limit(Config.LIMIT_NUM).toJavaRDD();
        }
    }

    /**
     * Configura i path per l'esecuzione locale oppure su container docker.
     */
    public static void setExecMode() {
        if (Config.EXEC_MODE.equals("LOCAL")) {
            jar_path = Config.LOCAL_JAR_PATH;
            dataset_path = Config.LOCAL_DATASET_PATH;
            spark_url = Config.LOCAL_SPARK_URL;
        } else {
            jar_path = Config.JAR_PATH;
            dataset_path = Config.DATASET_PATH;
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