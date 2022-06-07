package utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.CharacterIterator;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Scanner;
import java.util.TimeZone;

public class Tools {

    /**
     * Mette in attesa il programma fino all'inserimento di input utente
     */
    public static void promptEnterKey() {
        System.out.println("Running Spark WebUI on http://spark-master:4040/jobs/");
        System.out.println("Double press \"ENTER\" to end application...");
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
    }
    /**
     * Ritorna la tupla (method,occurrences) relativa al metodo di pagamento più usata nella fascia oraria
     *
     * @param list
     * @return
     */
    public static Tuple2<Long, Integer> getMostFrequentFromIterable(Iterable<Tuple2<Tuple2<Integer, Long>, Integer>> list) {
        Iterator<Tuple2<Tuple2<Integer, Long>, Integer>> iterator = list.iterator();

        Tuple2<Integer, Long> max = null;
        Integer maxVal = 0;

        while (iterator.hasNext()) {
            Tuple2<Tuple2<Integer, Long>, Integer> element = iterator.next();
            Tuple2<Integer, Long> actual = element._1();
            Integer actualVal = element._2();
            if (actualVal >= maxVal) {
                maxVal = actualVal;
                max = actual;
            }
        }
        return new Tuple2<>(max._2(), maxVal);
    }

    public static Integer getMonth(String timestamp) {
        return Integer.parseInt(timestamp.substring(5, 7));
    }

    public static Integer getHour(String timestamp) {
        return Integer.parseInt(timestamp.substring(11, 13));
    }

    public static Timestamp getTimestamp() {
        return new Timestamp(System.currentTimeMillis());
    }

    public static String toMinutes(long milliseconds) {
        long minutes = (milliseconds / 1000) / 60;
        long seconds = (milliseconds / 1000) % 60;
        return String.format("%d min, %d sec (%d ms)", minutes, seconds, milliseconds);
    }

    public static void printSystemSpecs() {
        System.out.println("\n————————————————————————————————————————————————————— Environment Specs —————————————————————————————————————————————————————");
        String nameOS = System.getProperty("os.name");
        String versionOS = System.getProperty("os.version");
        String architectureOS = System.getProperty("os.arch");
        System.out.println(String.format("OS Info: %s %s %s", nameOS, versionOS, architectureOS));

        /* Total number of processors or cores available to the JVM */
        System.out.println("Available processors (cores): " +
                Runtime.getRuntime().availableProcessors());

        /* Total amount of free memory available to the JVM */
        System.out.println("Free memory: " +
                byteToGB(Runtime.getRuntime().freeMemory()));

        /* This will return Long.MAX_VALUE if there is no preset limit */
        long maxMemory = Runtime.getRuntime().maxMemory();
        /* Maximum amount of memory the JVM will attempt to use */
        System.out.println("Maximum memory: " +
                (maxMemory == Long.MAX_VALUE ? "no limit" : byteToGB(maxMemory)));

        /* Total memory currently available to the JVM */
        System.out.println("Total memory available to JVM: " +
                byteToGB(Runtime.getRuntime().totalMemory()));

        System.out.println("Execution Mode: " + Config.EXEC_MODE);
        System.out.println("# Workers: " + Config.NUM_WORKERS);
        System.out.println("Data Mode: " + Config.DATA_MODE);

        System.out.println("—————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————");
    }

    public static String byteToGB(long bytes) {
        if (-1000 < bytes && bytes < 1000) {
            return bytes + " B";
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (bytes <= -999_950 || bytes >= 999_950) {
            bytes /= 1000;
            ci.next();
        }
        return String.format("%.1f %cB", bytes / 1000.0, ci.current());
    }
}
