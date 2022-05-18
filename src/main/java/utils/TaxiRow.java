package utils;

import org.apache.spark.sql.Row;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class TaxiRow {

    private long vendorID;
    private Timestamp tpep_pickup_datetime;
    private Timestamp tpep_dropoff_datetime;
    private double passenger;
    private double trip_distance;
    private double ratecodeID;
    private String store_and_fwd_flag;
    private Long PULocationID;
    private Long DOLocationID;
    private Long payment_type;
    private double fare_amount;
    private double extra;
    private double mta_tax;
    private double tip_amount;
    private double tolls_amount;
    private double improvement_surcharge;
    private double total_amount;
    private double congestion_surcharge;
    private double airport_fee;


    public TaxiRow() {
    }

    public void setVendorID(long vendorID) {
        this.vendorID = vendorID;
    }

    public void setTpep_pickup_datetime(Timestamp tpep_pickup_datetime) {
        this.tpep_pickup_datetime = tpep_pickup_datetime;
    }

    public void setTpep_dropoff_datetime(Timestamp tpep_dropoff_datetime) {
        this.tpep_dropoff_datetime = tpep_dropoff_datetime;
    }

    public void setPassenger(double passenger) {
        this.passenger = passenger;
    }

    public void setTrip_distance(double trip_distance) {
        this.trip_distance = trip_distance;
    }

    public void setRatecodeID(double ratecodeID) {
        this.ratecodeID = ratecodeID;
    }

    public void setStore_and_fwd_flag(String store_and_fwd_flag) {
        this.store_and_fwd_flag = store_and_fwd_flag;
    }

    public void setPULocationID(long PULocationID) {
        this.PULocationID = PULocationID;
    }

    public void setDOLocationID(long DOLocationID) {
        this.DOLocationID = DOLocationID;
    }

    public void setPayment_type(long payment_type) {
        this.payment_type = payment_type;
    }

    public void setFare_amount(double fare_amount) {
        this.fare_amount = fare_amount;
    }

    public void setExtra(double extra) {
        this.extra = extra;
    }

    public void setMta_tax(double mta_tax) {
        this.mta_tax = mta_tax;
    }

    public void setTip_amount(double tip_amount) {
        this.tip_amount = tip_amount;
    }

    public void setTolls_amount(double tolls_amount) {
        this.tolls_amount = tolls_amount;
    }

    public void setImprovement_surcharge(double improvement_surcharge) {
        this.improvement_surcharge = improvement_surcharge;
    }

    public void setTotal_amount(double total_amount) {
        this.total_amount = total_amount;
    }

    public void setCongestion_surcharge(double congestion_surcharge) {
        this.congestion_surcharge = congestion_surcharge;
    }

    public void setAirport_fee(double airport_fee) {
        this.airport_fee = airport_fee;
    }

    public long getVendorID() {
        return vendorID;
    }

    public Timestamp getTpep_pickup_datetime() {
        return tpep_pickup_datetime;
    }

    public Timestamp getTpep_dropoff_datetime() {
        return tpep_dropoff_datetime;
    }

    public double getPassenger() {
        return passenger;
    }

    public double getTrip_distance() {
        return trip_distance;
    }

    public double getRatecodeID() {
        return ratecodeID;
    }

    public String getStore_and_fwd_flag() {
        return store_and_fwd_flag;
    }

    public Long getPULocationID() {
        return PULocationID;
    }

    public Long getDOLocationID() {
        return DOLocationID;
    }

    public Long getPayment_type() {
        return payment_type;
    }

    public double getFare_amount() {
        return fare_amount;
    }

    public double getExtra() {
        return extra;
    }

    public double getMta_tax() {
        return mta_tax;
    }

    public double getTip_amount() {
        return tip_amount;
    }

    public double getTolls_amount() {
        return tolls_amount;
    }

    public double getImprovement_surcharge() {
        return improvement_surcharge;
    }

    public double getTotal_amount() {
        return total_amount;
    }

    public double getCongestion_surcharge() {
        return congestion_surcharge;
    }

    public double getAirport_fee() {
        return airport_fee;
    }

    public List<Double> getValues(){
        List<Double> doubles = Arrays.asList(getTip_amount(), getTotal_amount(), getTolls_amount());
        return doubles;
    }
    public static TaxiRow parse(Row row) {
        TaxiRow t = new TaxiRow();
        try {
            t.setVendorID(row.getLong(0));
            t.setTpep_pickup_datetime(row.getTimestamp(1));
            t.setTpep_dropoff_datetime(row.getTimestamp(2));
            t.setPassenger(row.getDouble(3));
            t.setTrip_distance(row.getDouble(4));
            t.setRatecodeID(row.getDouble(5));
            t.setStore_and_fwd_flag(row.getString(6));
            t.setPULocationID(row.getLong(7));
            t.setDOLocationID(row.getLong(8));
            t.setPayment_type(row.getLong(9));
            t.setFare_amount(row.getDouble(10));
            t.setExtra(row.getDouble(11));
            t.setMta_tax(row.getDouble(12));
            t.setTip_amount(row.getDouble(13));
            t.setTolls_amount(row.getDouble(14));
            t.setImprovement_surcharge(row.getDouble(15));
            t.setTotal_amount(row.getDouble(16));
            t.setCongestion_surcharge(row.getDouble(17));
            t.setAirport_fee((row.getDouble(18)));


        } catch (NullPointerException e) {
            // nothing
        }
        return t;
    }

    @Override
    public String toString() {
        return "TaxiRow{" +
                "vendorID=" + vendorID +
                ", tpep_pickup_datetime=" + tpep_pickup_datetime +
                ", tpep_dropoff_datetime=" + tpep_dropoff_datetime +
                ", passenger=" + passenger +
                ", trip_distance=" + trip_distance +
                ", ratecodeID=" + ratecodeID +
                ", store_and_fwd_flag=" + store_and_fwd_flag +
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
