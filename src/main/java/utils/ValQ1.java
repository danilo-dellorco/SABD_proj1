package utils;

import java.io.Serializable;

public class ValQ1 implements Serializable {
    Double tips;
    Integer occurrences;
    Long payment_type;
    Double tips_stddev;

    public ValQ1(Double tips, Integer occurrences) {
        setTips(tips);
        setOccurrences(occurrences);
    }

    public ValQ1(Double tips, Double tips_stddev, Integer occurrences) {
        setTips(tips);
        setTips_stddev(tips_stddev);
        setOccurrences(occurrences);
    }

    public ValQ1(Double tips, Integer occurrences, Long payment_type, Double tips_stddev) {
        this.tips = tips;
        this.occurrences = occurrences;
        this.payment_type = payment_type;
        this.tips_stddev = tips_stddev;
    }

    public ValQ1(Double tips_mean, Integer occurrences, Double tips_dev) {
        setTips(tips_mean);
        setOccurrences(occurrences);
        setTips_stddev(tips_dev);
    }

    @Override
    public String toString() {
        return "ValQ2{" +
                "tips=" + tips +
                ", occurrences=" + occurrences +
                ", payment_type=" + payment_type +
                ", tips_stddev=" + tips_stddev +
                '}';
    }

    public ValQ1(Double tips, Long payment_type, Integer occurrences) {
        setTips(tips);
        setTips_stddev(tips_stddev);
        setPayment_type(payment_type);
        setOccurrences(occurrences);
    }

    public Double getTips() {
        return tips;
    }

    public void setTips(Double tips) {
        this.tips = tips;
    }

    public Double getTips_stddev() {
        return tips_stddev;
    }

    public void setTips_stddev(Double tips_stddev) {
        this.tips_stddev = tips_stddev;
    }

    public Integer getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(Integer occurrences) {
        this.occurrences = occurrences;
    }

    public Long getPayment_type() {
        return payment_type;
    }

    public void setPayment_type(Long payment_type) {
        this.payment_type = payment_type;
    }

}


