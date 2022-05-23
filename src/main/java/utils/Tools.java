package utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.Scanner;

public class Tools {
    public static void printRDD(JavaRDD<TaxiRow> taxiRows){
        taxiRows.foreach((VoidFunction<TaxiRow>) r->System.out.println(r.toString()));
    }

    public static void promptEnterKey() {
        System.out.println("Running Spark WebUI on http://localhost:4040/jobs/");
        System.out.println("Double press \"ENTER\" to end application...");
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
    }

    public static TaxiRow ParseRow(Row r) {
        TaxiRow t = new TaxiRow();
        try {
            t.setTpep_dropoff_datetime(r.getTimestamp(0));
            t.setPassenger_count(r.getDouble(1));
            t.setDOLocationID(r.getLong(2));
            t.setPayment_type(r.getLong(3));
            t.setFare_amount(r.getDouble(4));
            t.setTip_amount(r.getDouble(5));
            t.setTolls_amount(r.getDouble(6));
            t.setTotal_amount(r.getDouble(7));
        } catch (NullPointerException e) {
            // Ignore rows with null fields
        }
        return t;
    }
}
