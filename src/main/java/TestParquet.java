
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.TaxiRow;

public class TestParquet {

    //private static String pathToFile = "data/reducedDataset.parquet";
    private static String pathToFile = "hdfs://hdfs-master:54310/yellow_tripdata_2021-12.parquet";
//    private static String pathToFile = "data/reduc/**/edDataset.parquet";
//    private static String pathToFile = "hdfs://hdfs-master:54310/yellow_tripdata_2021-12.parquet";

    public static void main(String[] args){

        SparkSession ss = SparkSession.builder()
                        .master("spark://spark-master:7077")
                                .appName("Test Parquet")
                                        .getOrCreate();

        Dataset<Row> parquetDec = ss.read().option("header", "false").parquet(pathToFile);
        ss.sparkContext().setLogLevel("ERROR");

        JavaRDD<Row> rddDec = parquetDec.toJavaRDD();
        JavaRDD<TaxiRow> taxiInfos = rddDec.map(row -> TaxiRow.parse(row));

        // tip/total_amount - toll_amount

        JavaRDD<Double> totals = taxiInfos.map(row -> row.getTotal_amount());
        JavaRDD<Double> tips = taxiInfos.map(row -> row.getTip_amount());
        JavaRDD<Double> tolls = taxiInfos.map(row->row.getTolls_amount());

        //TODO valutare caching

        double total_tips = tips.reduce((a,b) -> a+b);
        double total_amounts = totals.reduce((a,b)->a+b);
        double total_tolls = tolls.reduce((a,b)->a+b);

        System.out.println("Total_tips: "+total_tips + " Total_amounts: "+total_amounts+" Total_tolls: "+total_tolls);
        double mean = total_tips/(total_amounts-total_tolls);
        System.out.println("Mean: "+mean);

    }
}