package utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Scanner;

public class Tools {
    public static void printRDD(JavaRDD<TaxiRow> taxiRows){
        taxiRows.foreach((VoidFunction<TaxiRow>) r->System.out.println(r.toString()));
    }

    /**
     * Mette in attesa il programma fino all'inserimento di input utente
     */
    public static void promptEnterKey() {
        System.out.println("Running Spark WebUI on http://localhost:4040/jobs/");
        System.out.println("Double press \"ENTER\" to end application...");
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
    }

    /**
     * Genera un oggetto TaxiRow partendo da un Row generico del file parquet
     * @param r
     * @return
     */
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

    /**
     * Ritorna la tupla (method,occurrences) relativa al metodo di pagamento più usata nella fascia oraria
     * @param list
     * @return
     */
    public static Tuple2<Long,Integer> getMostFrequentFromIterable(Iterable<Tuple2<Tuple2<Integer, Long>, Integer>> list) {
        Iterator<Tuple2<Tuple2<Integer, Long>, Integer>> iterator = list.iterator();

        Tuple2<Integer,Long> max = null;
        Integer maxVal = 0;

        while (iterator.hasNext()){
            Tuple2<Tuple2<Integer, Long>, Integer> element = iterator.next();
            Tuple2<Integer,Long>  actual = element._1();
            Integer actualVal = element._2();
            if (actualVal>=maxVal){
                maxVal=actualVal;
                max = actual;
            }
        }
        return new Tuple2<>(max._2(),maxVal);
    }

    public static String toCSVLine(){
        return "";
    }

}
