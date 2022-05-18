package sparkSQL;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple3;
import examples.entities.Outlet;
import examples.entities.OutletParser;

public class  Query1Preprocessing {

    private static String pathToFile = "data/d14_filtered.csv";

    public static JavaRDD<Tuple3<String, String, Double>> preprocessDataset(JavaSparkContext sc) {


        JavaRDD<String> energyFile = sc.textFile(pathToFile);
        JavaRDD<Outlet> outlets =
                energyFile.map(
                        // line -> OutletParser.parseJson(line))         // JSON
                        line -> OutletParser.parseCSV(line))            // CSV
                        .filter(x -> x != null && x.getProperty().equals("1"));
        JavaRDD<Tuple3<String, String, Double>> result = outlets.map(x -> new Tuple3<String, String, Double>
                (x.getHouse_id(), x.getTimestamp(), Double.parseDouble(x.getValue())));


        return result;



    }
}
