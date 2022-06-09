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
import org.bson.Document;
import queries.Query;
import utils.map.Payment;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

public class Query2SQL extends Query {
    Dataset<Row> results;

    public Query2SQL(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection, String name) {
        super(spark, dataset, collection, name);
    }

    public Dataset<Row> createSchemaFromRDD(SparkSession spark, JavaRDD<Row> dataset) {
        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField("tpep_dropoff_datatime", DataTypes.TimestampType, true));
        fields.add(DataTypes.createStructField("do_location_id", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("payment_type", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("tip", DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);

        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        JavaRDD<Row> rowRDD = dataset.map((Function<Row, Row>)
                v1 ->{
                    Timestamp ts = v1.getTimestamp(0);
                    cal.setTime(ts);
                    Timestamp ts_zone = Timestamp.valueOf(sdf.format(cal.getTime()));
            return RowFactory.create(ts_zone, v1.getLong(1), v1.getLong(2), v1.getDouble(4));
                });
        return spark.createDataFrame(rowRDD, schema);
    }

    @Override
    public long execute() {
        Dataset<Row> data = createSchemaFromRDD(spark, dataset);
        data.createOrReplaceTempView("trip_infos");

        Dataset<Row> values = spark.sql("SELECT HOUR(tpep_dropoff_datatime) AS hour_slot, AVG(tip) AS tip_avg, STDDEV_POP(tip) AS tip_stddev " +
                "FROM trip_infos GROUP BY HOUR(tpep_dropoff_datatime) ORDER BY hour(tpep_dropoff_datatime) ASC");
        values.createOrReplaceTempView("values");

        Dataset<Row> paymentOccurrences = spark.sql("SELECT HOUR(tpep_dropoff_datatime) AS hour_slot, payment_type, COUNT(*) AS counted FROM trip_infos GROUP BY HOUR(tpep_dropoff_datatime), payment_type");
        paymentOccurrences.createOrReplaceTempView("occurrences");

        Dataset<Row> mostPopularPaymentType = spark.sql("SELECT hour_slot, payment_type, counted FROM occurrences table_1 WHERE counted =" +
                "(SELECT MAX(counted) FROM occurrences WHERE hour_slot = table_1.hour_slot) ORDER BY hour_slot ASC");

        mostPopularPaymentType.createOrReplaceTempView("mostPaymentType");

        results = spark.sql("SELECT mostPaymentType.hour_slot, payment_type, counted, tip_avg, tip_stddev " +
                "FROM mostPaymentType JOIN values ON mostPaymentType.hour_slot = values.hour_slot " +
                "ORDER BY mostPaymentType.hour_slot ASC");

        /**
         * Salvataggio dei risultati su mongodb
         */
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

            collection.insertOne(doc);
        }
        return 0;
    }

    @Override
    public void printResults() {
        System.out.println("\n———————————————————————————————————————————————————————— "+this.getName()+" ————————————————————————————————————————————————————————");
        results.show();
        System.out.print("—————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————\n");
    }
}

