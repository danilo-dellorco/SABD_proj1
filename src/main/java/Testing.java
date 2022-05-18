import examples.TaxiRow;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Testing {
    static int count_null = 0;
    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        String reducedDataset = "data/reducedDataset.parquet";
        String dataset1 = "data/yellow_tripdata_2021-12.parquet";
        String dataset2 = "data/yellow_tripdata_2022-01.parquet";
        String dataset3 ="data/yellow_tripdata_2022-02.parquet";


        JavaRDD<Row> parquetRDD_2 = spark.read().option("header","false").parquet(dataset2).toJavaRDD();
        JavaRDD<Row> parquetRDD_3 = spark.read().option("header","false").parquet(dataset3).toJavaRDD();

        query1_month(spark,dataset1,1);
        query1_month(spark,dataset2,2);
        query1_month(spark,dataset3,3);

        // Print all the examples.TaxiRow RDDs
        // taxiRows.foreach((VoidFunction<examples.TaxiRow>) r->System.out.println(r.toString()));
        // tip/(total_amount-tolls_amount)



        //System.out.println("\n\n\nRows with null fields: " + count_null);
    }
    public static TaxiRow ParseRow(Row r) {
        TaxiRow t = new TaxiRow();
        try {
            t.setVendorID(r.getLong(0));
            t.setTpep_pickup_datetime(r.getTimestamp(1));
            t.setTpep_dropoff_datetime(r.getTimestamp(2));
            t.setPassenger_count(r.getDouble(3));
            t.setTrip_distance(r.getDouble(4));
            t.setRatecodeID(r.getDouble(5));
            t.setStore_and_fwd_flag(r.getString(6));
            t.setPULocationID(r.getLong(7));
            t.setDOLocationID(r.getLong(8));
            t.setPayment_type(r.getLong(9));
            t.setFare_amount(r.getDouble(10));
            t.setExtra(r.getDouble(11));
            t.setMta_tax(r.getDouble(12));
            t.setTip_amount(r.getDouble(13));
            t.setTolls_amount(r.getDouble(14));
            t.setImprovement_surcharge(r.getDouble(15));
            t.setTotal_amount(r.getDouble(16));
            t.setCongestion_surcharge(r.getDouble(17));
            t.setAirport_fee(r.getDouble(18));
        } catch (NullPointerException e) {
            count_null++;
            // Skip Null
        }

        return t;
    }

    public static void query1_month(SparkSession spark, String dataset, int month){
        JavaRDD<Row> rowRDD = spark.read().option("header","false").parquet(dataset).toJavaRDD();
        JavaRDD<TaxiRow> taxiRows = rowRDD.map(r->ParseRow(r));

        // Analisys of Month 1
        JavaRDD<Double> tips = taxiRows.map(t->t.getTip_amount());
        JavaRDD<Double> total = taxiRows.map(t->t.getTotal_amount());
        JavaRDD<Double> tolls = taxiRows.map(t->t.getTolls_amount());

        double total_tips = tips.reduce((a,b)->a+b);
        double total_amount = total.reduce((a,b)->a+b);
        double tolls_amount = tolls.reduce((a,b)->a+b);

        double mean = total_tips/(total_amount-tolls_amount);
        System.out.println(String.format("Computed Mean for Month %d: ",month)+mean);
    }

}
