package queriesSQL;

import com.mongodb.client.MongoCollection;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import queries.Query;
import scala.Predef;
import utils.Config;
import utils.Payments;

import java.io.FileWriter;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Query2SQL extends Query {
    Dataset<Row> results;

    public Query2SQL(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection, String name) {
        super(spark, dataset, collection, name);
    }

    public Dataset<Row> createSchemaFromRDD(SparkSession spark, JavaRDD<Row> dataset) {
        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField("tpep_pickup_datatime", DataTypes.TimestampType, true));
        fields.add(DataTypes.createStructField("pu_location_id", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("payment_type", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("tip", DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);

        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        JavaRDD<Row> rowRDD = dataset.map((Function<Row, Row>)
                v1 ->{
                    Timestamp ts = v1.getTimestamp(0);
                    cal.setTime(ts);
                    Timestamp ts_zone = Timestamp.valueOf(sdf.format(cal.getTime()));
            return RowFactory.create(ts_zone, v1.getLong(2), v1.getLong(4), v1.getDouble(6));
                });


        return spark.createDataFrame(rowRDD, schema);
    }

    public void createZoneDataframe(){
        List<Row> range_zones = new ArrayList<>();
        for (int i = 1; i <266 ; i++) {
            range_zones.add(RowFactory.create(i));
        }

        JavaRDD<Row> zonesRDD = JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(range_zones);
        StructType schema = DataTypes.createStructType(new StructField[]{DataTypes.createStructField("zone_id", DataTypes.IntegerType, false)});
        Dataset<Row> zones = spark.createDataFrame(zonesRDD, schema);
        zones.createOrReplaceTempView("zones_id");
    }

    @Override
    public long execute() {
        Dataset<Row> data = createSchemaFromRDD(spark, dataset);
        data.createOrReplaceTempView("trip_infos");
//        createZoneDataframe();


        // {timestamp, zone}, trips, total_trip_per_hour, zone_perc
        Dataset<Row> scheduledTrips = spark.sql("SELECT timestamp, zone, trips, total_trip_hour, float(trips/total_trip_hour) as zone_perc FROM " +
                "(SELECT date_format(tpep_pickup_datatime, 'y-MM-dd HH') as timestamp, pu_location_id as zone, COUNT(*) as trips, avg(tip) " +
                "FROM trip_infos " +
                "GROUP BY timestamp, pu_location_id)" +
                "JOIN " +
                "(SELECT date_format(tpep_pickup_datatime, 'y-MM-dd HH') as timestamp_2, count(*) AS total_trip_hour from trip_infos group by timestamp_2)" +
                "ON timestamp = timestamp_2 ORDER BY timestamp ASC");
        scheduledTrips.show();
        scheduledTrips.createOrReplaceTempView("scheduled_trips");


        Dataset<Row> groupedTrips = spark.sql("SELECT timestamp, collect_list(concat_ws(':', zone, zone_perc)) as zone_percs FROM scheduled_trips GROUP BY timestamp");
        groupedTrips.createOrReplaceTempView("grouped_trips");

        // {timestamp}, trips, avg(tip), stddev(tip)
        Dataset<Row> hourly_values = spark.sql("SELECT date_format(tpep_pickup_datatime, 'y-MM-dd HH') as timestamp, COUNT(*) as trips, avg(tip) AS avg_tip, stddev_pop(tip) AS stddev_tip " +
                "FROM trip_infos " +
                "GROUP BY timestamp " +
                "ORDER BY timestamp ASC");
        hourly_values.createOrReplaceTempView("hourly_values");

        // {timestamp}, payment_type, occurrences
        Dataset<Row> paymentOccurrences = spark.sql("SELECT date_format(tpep_pickup_datatime, 'y-MM-dd HH') AS timestamp, payment_type, COUNT(*) AS counted " +
                        " FROM trip_infos GROUP BY timestamp, payment_type " +
                "ORDER BY timestamp ASC");
        paymentOccurrences.createOrReplaceTempView("payment_occurrences");

        // {timestamp}, most_popular_payment, payment_occurrences
        Dataset<Row> mostPopularPaymentType = spark.sql("SELECT timestamp, payment_type as most_popular_payment, counted AS payment_occurrences FROM  payment_occurrences table_1 WHERE counted =" +
                "(SELECT MAX(counted) FROM payment_occurrences WHERE timestamp = table_1.timestamp) ORDER BY timestamp ASC");
        mostPopularPaymentType.createOrReplaceTempView("most_popular_payment");

        // {timestamp}, avg_tip, stddev_tip, most_popular_payment
        results = spark.sql("SELECT table_1.timestamp AS timestamp, avg_tip, stddev_tip, most_popular_payment, string(zone_percs) AS trips_distribution, zone_percs AS percs_array FROM " +
                "(SELECT most_popular_payment.timestamp AS timestamp, avg_tip, stddev_tip, most_popular_payment FROM " +
                "hourly_values JOIN most_popular_payment ON hourly_values.timestamp = most_popular_payment.timestamp) table_1 " +
                "JOIN grouped_trips ON table_1.timestamp = grouped_trips.timestamp " +
                "ORDER BY timestamp ASC");
        results.drop("percs_array").coalesce(1).write().mode("overwrite").option("header", "true").csv(Config.HDFS_URL+"/Q2SQL");

        /*
         * Salvataggio dei risultati su mongodb

        List<Row> resultsList = results.collectAsList();
        for (Row r : resultsList){
            Integer payment = Integer.valueOf((int) r.getLong(1));      // Casting for bson documents
            Integer counted = Integer.valueOf((int) r.getLong(2));      // Casting for bson documents
            Document doc = new Document();
            doc.append("hour_slot", r.getInt(0));
            doc.append("payment_type", payment);
            doc.append("payment_name", Payment.staticMap.get(payment));
            doc.append("payment_occ", counted);
            doc.append("tip_avg", r.getDouble(3));
            doc.append("tip_stddev", r.getDouble(4));
            doc.append("tip_stddev", r.get);

            collection.insertOne(doc);


        }
        */
        return 0;
    }

    @Override
    public void printResults() {
        System.out.println("\n———————————————————————————————————————————————————————— "+this.getName()+" ————————————————————————————————————————————————————————");
        results.show();
        System.out.print("—————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————\n");
    }

    @Override
    public void writeResultsOnCSV() {
        String outputName = "Results/q2sql-res.csv";

        try (FileWriter fileWriter = new FileWriter(outputName)) {
//           List<String> csvLines = new
            StringBuilder outputBuilder = new StringBuilder("YYYY-MM-DD HH;");
            for (int i = 1; i <266 ; i++) {
                outputBuilder.append(String.format("perc_PU%d;", i));
            }
            outputBuilder.append("avg_tip;stddev_tip;pref_payment\n");
            fileWriter.append(outputBuilder.toString());

            for (Row row: results.collectAsList()){
                List<Double> percentuals = new ArrayList<>(Collections.nCopies(265, 0d));
                outputBuilder.setLength(0);                                     // Empty builder

                String timestamp = row.getString(0);
                Double avg_tip = row.getDouble(1);
                Double stddev_tip = row.getDouble(2);
                Long payment = row.getLong(3);
                List<String> distr = row.getList(5);
                for (String s: distr){
                    StringTokenizer tokenizer = new StringTokenizer(s, ":");
                    int id = Integer.parseInt(tokenizer.nextToken());
                    Double value = Double.valueOf(tokenizer.nextToken());
                    percentuals.set(id-1, value);
                }
                String percentualStrings = percentuals.toString().replace(",", ";").substring(1, percentuals.toString().length()-1);
                outputBuilder.append(String.format("%s;%s;%f;%f;%d\n", timestamp, percentualStrings, avg_tip, stddev_tip, payment));
                fileWriter.append(outputBuilder.toString());
            }

//            System.out.println(percentuals);
        } catch (Exception e) {
            System.out.println("Results CSV Error: " + e.toString());
        }


    }
}

