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
import org.bson.Document;
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
                "FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY day ORDER BY dest_for_day DESC) AS top FROM values) " +
                "WHERE top <= 5");
        top5_per_day.createOrReplaceTempView("top5_per_day");
        //top5_per_day.show();

        Dataset<Row> merged_days = spark.sql("SELECT day, COLLECT_LIST(destination) AS dest, COLLECT_LIST(passenger_avg) AS pass, COLLECT_LIST(fare_avg) AS fare, COLLECT_LIST(fare_stddev) AS stddev " +
                "FROM top5_per_day GROUP BY day");
        merged_days.createOrReplaceTempView("merged_days");
        //merged_days.show();

        // define, register and use udf function to swap zone code with zone name using java map
        spark.udf().register("setZones", (UDF1<String, String>) id -> Zone.zoneMap.get(Integer.parseInt(id)), DataTypes.StringType);

        // results with zones_id casted to string because they will host the zone string name instead of the id
        results = spark.sql("SELECT day, setZones(CAST(ELEMENT_AT(dest, 1) AS String)) AS D01, setZones(CAST(ELEMENT_AT(dest, 2) AS String)) AS D02, setZones(CAST(ELEMENT_AT(dest, 3) AS String)) AS D03, setZones(CAST(ELEMENT_AT(dest, 4) AS String)) AS D04, setZones(CAST(ELEMENT_AT(dest, 5) AS String)) AS D05, " +
                "ELEMENT_AT(pass, 1) AS avg_pax_D01, ELEMENT_AT(pass, 2) AS avg_pax_D02, ELEMENT_AT(pass, 3) AS avg_pax_D03, ELEMENT_AT(pass, 4) AS avg_pax_D04, ELEMENT_AT(pass, 5) AS avg_pax_D05, " +
                "ELEMENT_AT(fare, 1) AS avg_fare_D01, ELEMENT_AT(fare, 2) AS avg_fare_D02, ELEMENT_AT(fare, 3) AS avg_fare_D03, ELEMENT_AT(fare, 4) AS avg_fare_D04, ELEMENT_AT(fare, 5) AS avg_fare_D05, " +
                "ELEMENT_AT(stddev, 1) AS avg_stddev_D01, ELEMENT_AT(stddev, 2) AS avg_stddev_D02, ELEMENT_AT(stddev, 3) AS avg_stddev_D03, ELEMENT_AT(stddev, 4) AS avg_stddev_D04, ELEMENT_AT(stddev, 5) AS avg_stddev_D05 " +
                "FROM merged_days");

        results.coalesce(1).write().mode("overwrite").option("header", "true")
                .csv(Config.HDFS_URL+"/Q3SQL");
        /**
         * Salvataggio dei risultati su mongodb
         */

        List<Row> resultsList = results.collectAsList();
        for (Row r : resultsList) {
            Document doc = new Document();
            doc.append("day", r.getDate(0));
            doc.append("D01", r.getString(1));
            doc.append("D02", r.getString(2));
            doc.append("D03", r.getString(3));
            doc.append("D04", r.getString(4));
            doc.append("D05", r.getString(5));
            doc.append("avg_pax_D01", r.getDouble(6));
            doc.append("avg_pax_D02", r.getDouble(7));
            doc.append("avg_pax_D03", r.getDouble(8));
            doc.append("avg_pax_D04", r.getDouble(9));
            doc.append("avg_pax_D05", r.getDouble(10));
            doc.append("avg_fare_D01", r.getDouble(11));
            doc.append("avg_fare_D02", r.getDouble(12));
            doc.append("avg_fare_D03", r.getDouble(13));
            doc.append("avg_fare_D04", r.getDouble(14));
            doc.append("avg_fare_D05", r.getDouble(15));
            doc.append("avg_stddev_D01", r.getDouble(16));
            doc.append("avg_stddev_D02", r.getDouble(17));
            doc.append("avg_stddev_D03", r.getDouble(18));
            doc.append("avg_stddev_D04", r.getDouble(19));
            doc.append("avg_stddev_D05", r.getDouble(20));
            collection.insertOne(doc);
        }
    }

    @Override
    public void printResults() {
        System.out.println("\n———————————————————————————————————————————————————————— "+this.getName()+" ————————————————————————————————————————————————————————");
        results.show();
        System.out.print("—————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————\n");
    }
}

