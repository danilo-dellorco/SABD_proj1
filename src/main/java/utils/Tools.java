package utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;

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
            t.setVendorID(r.getLong(0));
            t.setTpep_pickup_datetime(r.getTimestamp(1));
            t.setTpep_dropoff_datetime(r.getTimestamp(2));
            t.setPassenger_count(r.getDouble(3));
            t.setTrip_distance(r.getDouble(4));
            t.setRatecodeID(r.getDouble(5));
            t.setStore_and_fwd_flag(r.getString(6));
            t.setPULocationID(r.getLong(7));
            t.setDOLocationID(r.getLong(8));
            t.setPayment_type(r.getLong(9));
            t.setFare_amount(r.getDouble(10));
            t.setExtra(r.getDouble(11));
            t.setMta_tax(r.getDouble(12));
            t.setTip_amount(r.getDouble(13));
            t.setTolls_amount(r.getDouble(14));
            t.setImprovement_surcharge(r.getDouble(15));
            t.setTotal_amount(r.getDouble(16));
            t.setCongestion_surcharge(r.getDouble(17));
            t.setAirport_fee(r.getDouble(18));
        } catch (NullPointerException e) {
            // Ignore rows with null fields
        }
        return t;
    }
}
