package utils;

public class Config {
    public static final String EXEC_MODE = "DOCKER"; //{LOCAL,DOCKER}
    public static final String DATA_MODE = "UNLIMITED"; //{LIMITED,UNLIMITED}
    public static final int LIMIT_NUM = 10000;
    public static final String NUM_WORKERS = "3";
    public static final String LOCAL_SPARK_URL = "local["+NUM_WORKERS+"]";
    public static final String LOCAL_DATA_URL = "data";

    public static final String SPARK_URL = "spark://spark-master:7077";
    public static final String HDFS_URL = "hdfs://hdfs-master:54310";

    public static final String DATASET = "/filtered.parquet";
    public static final String DATASET_PATH = HDFS_URL + DATASET;
    public static final String LOCAL_DATASET_PATH = LOCAL_DATA_URL + DATASET;
    public static final String JAR_PATH = HDFS_URL + "/sabd-proj-1.0.jar";
    public static final String LOCAL_JAR_PATH = "target" + "/sabd-proj-1.0.jar";
    public static final String MONGO_URL  = "mongodb://mongo-server:27017";
    public static final String MONGO_DB   = "sabd1";
    public static final String MONGO_Q1   = "q1_res";
    public static final String MONGO_Q2   = "q2_res";
    public static final String MONGO_Q3   = "q3_results";
    public static final String MONGO_Q1SQL = "q1sql_res";
    public static final String MONGO_Q2SQL = "q2sql_res";
    public static final String MONGO_Q3SQL   = "q3sql_res";
}
