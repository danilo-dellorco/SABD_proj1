package utils;

import java.io.Serializable;

public class GreenTaxiRow implements Serializable {
    long VendorID;
    String tpep_pickup_datetime;
    String tpep_dropoff_datetime;
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

    public GreenTaxiRow() {
    }

    @Override
    public String toString() {
        return "utils.TaxiRow{" +
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

    public long getVendorID() {
        return VendorID;
    }

    public String getTpep_pickup_datetime() {
        return tpep_pickup_datetime;
    }

    public String getTpep_dropoff_datetime() {
        return tpep_dropoff_datetime;
    }

    public Double getPassenger_count() {
        return passenger_count;
    }

    public Double getTrip_distance() {
        return trip_distance;
    }

    public Double getRatecodeID() {
        return RatecodeID;
    }

    public String getStore_and_fwd_flag() {
        return store_and_fwd_flag;
    }

    public long getPULocationID() {
        return PULocationID;
    }

    public long getDOLocationID() {
        return DOLocationID;
    }

    public long getPayment_type() {
        return payment_type;
    }

    public Double getFare_amount() {
        return fare_amount;
    }

    public Double getExtra() {
        return extra;
    }

    public Double getMta_tax() {
        return mta_tax;
    }

    public Double getTip_amount() {
        if (tip_amount!=null){
            return tip_amount;
        }
        return 0.0;
    }

    public Double getTolls_amount() {
        if (tolls_amount!=null){
            return tolls_amount;
        }
        return 0.0;
    }

    public Double getImprovement_surcharge() {
        return improvement_surcharge;
    }

    public Double getTotal_amount() {
        if (total_amount!=null){
            return total_amount;
        }
        return 0.0;
    }

    public Double getCongestion_surcharge() {
        return congestion_surcharge;
    }

    public Double getAirport_fee() {
        return airport_fee;
    }

    public void setVendorID(long vendorID) {
        VendorID = vendorID;
    }

    public void setTpep_pickup_datetime(String tpep_pickup_datetime) {
        this.tpep_pickup_datetime = tpep_pickup_datetime;
    }

    public void setTpep_dropoff_datetime(String tpep_dropoff_datetime) {
        this.tpep_dropoff_datetime = tpep_dropoff_datetime;
    }

    public void setPassenger_count(Double passenger_count) {
        this.passenger_count = passenger_count;
    }

    public void setTrip_distance(Double trip_distance) {
        this.trip_distance = trip_distance;
    }

    public void setRatecodeID(Double ratecodeID) {
        RatecodeID = ratecodeID;
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

    public void setFare_amount(Double fare_amount) {
        this.fare_amount = fare_amount;
    }

    public void setExtra(Double extra) {
        this.extra = extra;
    }

    public void setMta_tax(Double mta_tax) {
        this.mta_tax = mta_tax;
    }

    public void setTip_amount(Double tip_amount) {
        this.tip_amount = tip_amount;
    }

    public void setTolls_amount(Double tolls_amount) {
        this.tolls_amount = tolls_amount;
    }

    public void setImprovement_surcharge(Double improvement_surcharge) {
        this.improvement_surcharge = improvement_surcharge;
    }

    public void setTotal_amount(Double total_amount) {
        this.total_amount = total_amount;
    }

    public void setCongestion_surcharge(Double congestion_surcharge) {
        this.congestion_surcharge = congestion_surcharge;
    }

    public void setAirport_fee(Double airport_fee) {
        this.airport_fee = airport_fee;
    }
}


