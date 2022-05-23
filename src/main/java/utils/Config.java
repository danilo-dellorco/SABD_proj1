package utils;

public class Config {
    public static final String SPARK_URL = "local";
    public static final String HDFS_URL = "data";

    //public static final String SPARK_URL = "spark://spark-master:7077";
    //public static final String HDFS_URL = "hdfs://hdfs-master:54310";

    public static final String DAT1_PATH = HDFS_URL + "/yellow_tripdata_2021-12.parquet";
    public static final String DAT2_PATH = HDFS_URL + "/yellow_tripdata_2022-01.parquet";
    public static final String DAT3_PATH = HDFS_URL + "/yellow_tripdata_2022-02.parquet";

    public static final String MONGO_URL  = "localhost";
    public static final int    MONGO_PORT = 27017;
    public static final String MONGO_DB   = "SABD_proj1";
    public static final String MONGO_Q1   = "Q1_results";
    public static final String MONGO_Q2   = "Q2_results";
    public static final String MONGO_Q3   = "Q3_results";
}
