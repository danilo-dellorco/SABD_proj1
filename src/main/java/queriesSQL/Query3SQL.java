package queriesSQL;

import com.mongodb.client.MongoCollection;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import queries.Query;

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

        fields.add(DataTypes.createStructField("tpep_dropoff_datatime", DataTypes.TimestampType, true));
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
                    //tocca rimette le colonne che servono del dataset e poi modifica ste get che cambiano gli indici
                    Timestamp ts = v1.getTimestamp(0);
                    cal.setTime(ts);
                    Timestamp ts_zone = Timestamp.valueOf(sdf.format(cal.getTime()));
            return RowFactory.create(ts_zone, v1.getLong(1), v1.getDouble(7), v1.getDouble(3));
                });
        return spark.createDataFrame(rowRDD, schema);
    }

    @Override
    public void execute() {
        Dataset<Row> data = createSchemaFromRDD(spark, dataset);
        data.createOrReplaceTempView("trip_infos");

        Dataset<Row> values = spark.sql("SELECT DATE(tpep_dropoff_datatime) AS day, do_location_id AS destination, AVG(passenger_count) AS passenger_avg, AVG(fare_amount) as fare_avg, STDDEV_POP(fare_amount) AS fare_stddev, COUNT(*) AS dest_for_day " +
                "FROM trip_infos GROUP BY day, destination ORDER BY day, dest_for_day DESC");
        values.createOrReplaceTempView("values");

        results = spark.sql("SELECT day, destination, dest_for_day, passenger_avg, fare_avg, fare_stddev " +
                "FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY day ORDER BY dest_for_day DESC) AS top5 FROM values) " +
                "WHERE top5 <= 5");

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

