package utils;

public class Config {
    public static final String EXEC_MODE = "LOCAL"; //{LOCAL,DOCKER}
    public static final String DATA_MODE = "UNLIMITED"; //{LIMITED,UNLIMITED}
    public static final int LIMIT_NUM = 1000;

    public static final String NUM_WORKERS = "1";
    public static final String LOCAL_SPARK_URL = "local["+NUM_WORKERS+"]";
    public static final String LOCAL_DATA_URL = "data";

    public static final String SPARK_URL = "spark://spark-master:7077";
    public static final String HDFS_URL = "hdfs://hdfs-master:54310";


//    public static final String YELLOW_DATASET = "/filtered.parquet";
    public static final String YELLOW_DATASET = "/filtered.parquet";
    public static final String GREEN_DATASET = "/green.parquet";

    public static final String YELLOW_DATASET_PATH = HDFS_URL + YELLOW_DATASET;
    public static final String LOCAL_YELLOW_DATASET_PATH = LOCAL_DATA_URL + YELLOW_DATASET;

    public static final String GREEN_DATASET_PATH = HDFS_URL + GREEN_DATASET;
    public static final String LOCAL_GREEN_DATASET_PATH = LOCAL_DATA_URL + GREEN_DATASET;

    public static final String DAT1_PATH = HDFS_URL + "/filtered-dec.parquet";
    public static final String LOCAL_DAT1_PATH = LOCAL_DATA_URL + "/filtered-dec.parquet";
    public static final String DAT2_PATH = HDFS_URL + "/yellow_tripdata_2022-01.parquet";
    public static final String DAT3_PATH = HDFS_URL + "/yellow_tripdata_2022-02.parquet";

    public static final String JAR_PATH = HDFS_URL + "/sabd-proj-1.0.jar";
    public static final String LOCAL_JAR_PATH = "target" + "/sabd-proj-1.0.jar";

    public static final String MONGO_URL  = "mongodb://mongo-server:27017";
    public static final String MONGO_DB   = "sabd1";
    public static final String MONGO_Q1   = "q1_res";
    public static final String MONGO_Q2   = "q2_res";
    public static final String MONGO_Q3   = "q3_results";
    public static final String MONGO_Q4   = "q4_res";
    public static final String MONGO_Q1SQL = "q1sql_res";
    public static final String MONGO_Q2SQL = "q2_sql_res";
}
