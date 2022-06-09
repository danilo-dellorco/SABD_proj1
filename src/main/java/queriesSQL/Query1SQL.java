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
import utils.Config;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

public class Query1SQL extends Query {
    Dataset<Row> results;
    public Query1SQL(SparkSession spark, JavaRDD<Row> dataset, MongoCollection collection, String name) {
        super(spark, dataset, collection, name);
    }

    public Dataset<Row> createSchemaFromRDD(SparkSession spark, JavaRDD<Row> dataset) {
        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField("tpep_dropoff_datatime", DataTypes.TimestampType, true));
        fields.add(DataTypes.createStructField("tip_amount", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("tolls_amount", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("total_amount", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("payment_type", DataTypes.LongType, true));
        StructType schema = DataTypes.createStructType(fields);

        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        JavaRDD<Row> rowRDD = dataset.map((Function<Row, Row>)
                v1 -> {
                    Timestamp ts = v1.getTimestamp(1);
                    cal.setTime(ts);

                    Timestamp ts_zone = Timestamp.valueOf(sdf.format(cal.getTime()));
                    return RowFactory.create(ts_zone, v1.getDouble(6), v1.getDouble(7), v1.getDouble(8), v1.getLong(4));
                });

        return spark.createDataFrame(rowRDD, schema);
    }

    @Override
    public long execute() {
        Dataset<Row> data = createSchemaFromRDD(spark, dataset);
        data.createOrReplaceTempView("taxi_row");

        Dataset<Row> values = spark.sql("SELECT date_format(tpep_dropoff_datatime, 'y/MM') AS date, " +
                "sum(tip_amount) AS tips, sum(tolls_amount) AS tolls, sum(total_amount) AS total, " +
                "count(*) AS trips_number " +
                "FROM taxi_row WHERE payment_type = 1 GROUP BY date_format(tpep_dropoff_datatime, 'y/MM')");

//        values.show();
        values.createOrReplaceTempView("taxi_values");
        results = spark.sql("SELECT date, " +                                         // month-1 per riportare alla notazione originale 0-11
                " (tips/(total-tolls)) AS tips_percentage, trips_number FROM taxi_values ORDER BY date ASC");       //date_format(to_timestamp(string(month), 'M'), 'MMMM')  per convertire il mese in nome stringa


        results.coalesce(1).write().mode("overwrite").option("header", "true").csv(Config.HDFS_URL+"/Q1SQL  ");
        /**
         * Salvataggio dei risultati su mongodb
         */
        List<Row> resultsList = results.collectAsList();
        for (Row r : resultsList){
            Document doc = new Document();
            doc.append("month_id", r.getString(0));
            doc.append("tips_percentage", Double.valueOf((int) r.getDouble(1)));
            doc.append("trips_number", r.getLong(2));

            collection.insertOne(doc);
        }
        return 0;
    }

    @Override
    public void printResults() {
        System.out.println("\n———————————————————————————————————————————————————————— "+this.getName()+" ————————————————————————————————————————————————————————");
        results.show();
        System.out.printf("—————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————\n");
    }
}
