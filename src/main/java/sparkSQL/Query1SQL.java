package sparkSQL;

import com.mongodb.client.MongoCollection;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import queries.Query;
import scala.Tuple3;
import utils.Month;
import utils.TaxiRow;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

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

        JavaRDD<Row> rowRDD = dataset.map((Function<Row, Row>)
                v1 -> RowFactory.create(v1.getTimestamp(0), v1.getDouble(5), v1.getDouble(6), v1.getDouble(7)));

        return spark.createDataFrame(rowRDD, schema);
    }

    @Override
    public void execute() {
        Dataset<Row> data = createSchemaFromRDD(spark, dataset);
        data.createOrReplaceTempView("taxi_values");
        System.out.println("ROWS: "+data.count());
        Dataset<Row> values = spark.sql("SELECT month(tpep_dropoff_datatime) AS month, " +
                "sum(tip_amount) AS tips, sum(tolls_amount) AS tolls, sum(total_amount) AS total, " +
                "count(*) AS occurrences " +
                "FROM taxi_values GROUP BY month(tpep_dropoff_datatime)");
        values.createOrReplaceTempView("taxi_values");
//        values.show();
        Dataset<Row> results = spark.sql("SELECT month-1 AS month, " +                                         // month-1 per riportare alla notazione originale 0-11
                        "date_format(to_timestamp(string(month), 'M'), 'MMMMM') AS month_name," +
                        " (tips/(total-tolls)) AS mean FROM taxi_values");

        results.show();

//        METODO ALTERNATIVO utilizzando metodi sql integrati di spark
//        values.withColumn("mean", values.col("tips").divide((values.col("total").minus(values.col("tolls"))))).show();
    }

    @Override
    public void print() {

    }
}
