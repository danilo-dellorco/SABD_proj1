import org.apache.spark.sql.Row;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;

public class TaxiRow {
    long VendorID;
    Timestamp tpep_pickup_datetime;
    Timestamp tpep_dropoff_datetime;
    Double passenger_count;
    Double trip_distance;
    Double RatecodeID;
    String store_and_fwd_flag;
    long PULocationID;
    long DOLocationID;
    long payment_type;
    Double fare_amount;
    Double extra;
    Double mta_tax;
    Double tip_amount;
    Double tolls_amount;
    Double improvement_surcharge;
    Double total_amount;
    Double congestion_surcharge;
    Double airport_fee;

    public TaxiRow() {
    }

    @Override
    public String toString() {
        return "TaxiRow{" +
                "VendorID=" + VendorID +
                ", tpep_pickup_datetime=" + tpep_pickup_datetime +
                ", tpep_dropoff_datetime=" + tpep_dropoff_datetime +
                ", passenger_count=" + passenger_count +
                ", trip_distance=" + trip_distance +
                ", RatecodeID=" + RatecodeID +
                ", store_and_fwd_flag='" + store_and_fwd_flag + '\'' +
                ", PULocationID=" + PULocationID +
                ", DOLocationID=" + DOLocationID +
                ", payment_type=" + payment_type +
                ", fare_amount=" + fare_amount +
                ", extra=" + extra +
                ", mta_tax=" + mta_tax +
                ", tip_amount=" + tip_amount +
                ", tolls_amount=" + tolls_amount +
                ", improvement_surcharge=" + improvement_surcharge +
                ", total_amount=" + total_amount +
                ", congestion_surcharge=" + congestion_surcharge +
                ", airport_fee=" + airport_fee +
                '}';
    }
}


