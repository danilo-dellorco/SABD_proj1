package queriesSQL;

import com.mongodb.client.MongoCollection;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import queries.Query;
import utils.Config;
import utils.Zone;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

public class Query3SQL extends Query {
    Dataset<Row> results;

    public Query3SQL(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection, String name) {
        super(spark, dataset, collection, name);
    }

    public Dataset<Row> createSchemaFromRDD(SparkSession spark, JavaRDD<Row> dataset) {
        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField("tpep_dropoff_datetime", DataTypes.TimestampType, true));
        fields.add(DataTypes.createStructField("do_location_id", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("passenger_count", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("fare_amount", DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);

        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        JavaRDD<Row> rowRDD = dataset.map((Function<Row, Row>)
                v1 ->{
                    Timestamp ts = v1.getTimestamp(1);
                    cal.setTime(ts);
                    Timestamp ts_zone = Timestamp.valueOf(sdf.format(cal.getTime()));
            return RowFactory.create(ts_zone, v1.getLong(3), v1.getDouble(9), v1.getDouble(5));
                });
        return spark.createDataFrame(rowRDD, schema);
    }

    @Override
    public void execute() {
        Dataset<Row> data = createSchemaFromRDD(spark, dataset);
        data.createOrReplaceTempView("trip_infos");

        Dataset<Row> values = spark.sql("SELECT DATE(tpep_dropoff_datetime) AS day, do_location_id AS destination, AVG(passenger_count) AS passenger_avg, AVG(fare_amount) as fare_avg, STDDEV_POP(fare_amount) AS fare_stddev, COUNT(*) AS dest_for_day " +
                "FROM trip_infos GROUP BY day, destination ORDER BY day, dest_for_day DESC");
        values.createOrReplaceTempView("values");

        Dataset<Row> top5_per_day = spark.sql("SELECT day, destination, dest_for_day, passenger_avg, fare_avg, fare_stddev " +
                "FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY day ORDER BY dest_for_day DESC) AS top5 FROM values) " +
                "WHERE top5 <= 5");
        top5_per_day.createOrReplaceTempView("top5_per_day");
        top5_per_day.show();

        Dataset<Row> merged_days = spark.sql("SELECT day, COLLECT_LIST(destination) AS dest, COLLECT_LIST(passenger_avg) AS pass, COLLECT_LIST(fare_avg) AS fare, COLLECT_LIST(fare_stddev) AS stddev " +
                "FROM top5_per_day GROUP BY day");
        merged_days.createOrReplaceTempView("merged_days");

        // results with zone codes, also casted to string because they will later host the real string name
        Dataset<Row> view = spark.sql("SELECT day, CAST(ELEMENT_AT(dest, 1) AS String) AS D01, CAST(ELEMENT_AT(dest, 2) AS String) AS D02, CAST(ELEMENT_AT(dest, 3) AS String) AS D03, CAST(ELEMENT_AT(dest, 4) AS String) AS D04, CAST(ELEMENT_AT(dest, 5) AS String) AS D05, " +
                "ELEMENT_AT(pass, 1) AS avg_pax_D01, ELEMENT_AT(pass, 2) AS avg_pax_D02, ELEMENT_AT(pass, 3) AS avg_pax_D03, ELEMENT_AT(pass, 4) AS avg_pax_D04, ELEMENT_AT(pass, 5) AS avg_pax_D05, " +
                "ELEMENT_AT(fare, 1) AS avg_fare_D01, ELEMENT_AT(pass, 2) AS avg_fare_D02, ELEMENT_AT(pass, 3) AS avg_fare_D03, ELEMENT_AT(pass, 4) AS avg_fare_D04, ELEMENT_AT(pass, 5) AS avg_fare_D05, " +
                "ELEMENT_AT(stddev, 1) AS avg_stddev_D01, ELEMENT_AT(stddev, 2) AS avg_stddev_D02, ELEMENT_AT(stddev, 3) AS avg_stddev_D03, ELEMENT_AT(stddev, 4) AS avg_stddev_D04, ELEMENT_AT(stddev, 5) AS avg_stddev_D05 " +
                "FROM merged_days");
        view.createOrReplaceTempView("view");
        view.show();
        System.out.println("\n\n");

        // define, register and use udf function to swap zone code with zone name using java map
        spark.udf().register("setZones", (UDF1<String, String>) id -> Zone.zoneMap.get(Integer.parseInt(id)), DataTypes.StringType);
        results = spark.sql("SELECT day, setZones(D01) AS D01, setZones(D02) AS D02, setZones(D03) AS D03, setZones(D04) AS D04, setZones(D05) AS D05, " +
                "avg_pax_D01, avg_pax_D02, avg_pax_D03, avg_pax_D04, avg_pax_D05, avg_fare_D01, avg_fare_D02, avg_fare_D03, " +
                "avg_fare_D04, avg_fare_D05, avg_stddev_D01, avg_stddev_D02, avg_stddev_D03, avg_stddev_D04, avg_stddev_D05 " +
                "FROM view");

        results.coalesce(1).write().mode("overwrite").option("header", "true")
                .csv(Config.HDFS_URL+"/Q3SQL");
        /**
         * Salvataggio dei risultati su mongodb
         */
        /*
        List<Row> resultsList = results.collectAsList();
        for (Row r : resultsList){
            Integer payment = Integer.valueOf((int) r.getLong(1));      // Casting for bson documents
            Integer counted = Integer.valueOf((int) r.getLong(2));      // Casting for bson documents
            Document doc = new Document();
            doc.append("hour_slot", r.getInt(0));
            doc.append("payment_type", payment);
            doc.append("payment_name", Payments.staticMap.get(payment));
            doc.append("payment_occ", counted);
            doc.append("tip_avg", r.getDouble(3));
            doc.append("tip_stddev", r.getDouble(4));

            collection.insertOne(doc);
        }
         */
    }

    @Override
    public void printResults() {
        System.out.println("\n———————————————————————————————————————————————————————— "+this.getName()+" ————————————————————————————————————————————————————————");
        results.show();
        System.out.print("—————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————\n");
    }
}
