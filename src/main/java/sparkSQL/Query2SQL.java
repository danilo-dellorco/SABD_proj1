package sparkSQL;

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

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;

public class Query2SQL extends Query {
    public Query2SQL(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection) {
        super(spark, dataset, collection);
    }

    public Dataset<Row> createSchemaFromRDD(SparkSession spark, JavaRDD<Row> dataset) {
        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField("tpep_dropoff_datatime", DataTypes.TimestampType, true));
        fields.add(DataTypes.createStructField("do_location_id", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("payment_type", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("tip", DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = dataset.map((Function<Row, Row>)
                v1 -> RowFactory.create(v1.getTimestamp(0), v1.getLong(2), v1.getLong(3), v1.getDouble(5)));

        return spark.createDataFrame(rowRDD, schema);
    }

    @Override
    public void execute() {
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

        Dataset<Row> results = spark.sql("SELECT mostPaymentType.hour_slot, payment_type, counted, tip_avg, tip_stddev " +
                "FROM mostPaymentType JOIN values ON mostPaymentType.hour_slot = values.hour_slot ORDER BY mostPaymentType.hour_slot ASC");
        results.show();
    }

    @Override
    public void print() {

    }
}
