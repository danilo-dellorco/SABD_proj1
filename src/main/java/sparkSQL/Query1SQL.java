package sparkSQL;

import com.mongodb.client.FindIterable;
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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

public class Query1SQL extends Query {
    public Query1SQL(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection) {
        super(spark, dataset, collection);
    }

    public Dataset<Row> createSchemaFromRDD(SparkSession spark, JavaRDD<Row> dataset) {
        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField("tpep_dropoff_datatime", DataTypes.TimestampType, true));
        fields.add(DataTypes.createStructField("tip_amount", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("tolls_amount", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("total_amount", DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);

        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        JavaRDD<Row> rowRDD = dataset.map((Function<Row, Row>)
                v1 -> {
                    Timestamp ts = v1.getTimestamp(0);
                    cal.setTime(ts);
                    Timestamp ts_zone = Timestamp.valueOf(sdf.format(cal.getTime()));
                    return RowFactory.create(ts_zone, v1.getDouble(4), v1.getDouble(5), v1.getDouble(6));
                });

        return spark.createDataFrame(rowRDD, schema);
    }

    @Override
    public void execute() {
        Dataset<Row> data = createSchemaFromRDD(spark, dataset);
        data.createOrReplaceTempView("taxi_row");
        Dataset<Row> values = spark.sql("SELECT month(tpep_dropoff_datatime) AS month, " +
                "sum(tip_amount) AS tips, sum(tolls_amount) AS tolls, sum(total_amount) AS total, " +
                "count(*) AS occurrences " +
                "FROM taxi_row GROUP BY month(tpep_dropoff_datatime)");
        values.createOrReplaceTempView("taxi_values");
        Dataset<Row> results = spark.sql("SELECT month AS month_id, " +                                         // month-1 per riportare alla notazione originale 0-11
                "date_format(to_timestamp(string(month), 'M'), 'MMMM') AS month_name," +
                " (tips/(total-tolls)) AS mean FROM taxi_values ORDER BY month ASC");


        /**
         * Salvataggio dei risultati su mongodb
         */
        List<Row> resultsList = results.collectAsList();
        for (Row r : resultsList){
            Document doc = new Document();
            doc.append("month_id", r.getInt(0));
            doc.append("month_name", r.getString(1));
            doc.append("mean", r.getDouble(2));

            collection.insertOne(doc);
        }

//        METODO ALTERNATIVO utilizzando metodi sql integrati di spark
//        values.withColumn("mean", values.col("tips").divide((values.col("total").minus(values.col("tolls"))))).show();
    }
}
