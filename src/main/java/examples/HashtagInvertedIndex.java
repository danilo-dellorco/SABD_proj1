package examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import examples.entities.Tweet;
import examples.entities.TweetParser;

import java.util.*;
import java.util.regex.Pattern;

/**
 *  Buildind a hashtag inverted index.
 *
 *  An inverted is the data structure used to quickly retrieves the list of content related to a keyword.
 *  We create an inverted index that maps a hashtag to the users who tweeted that hashtag
 */
public class HashtagInvertedIndex {

    private static String pathToFile = "data/reduced-tweets.json";
    private static Pattern SPACE = Pattern.compile(" ");


    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Inverted index");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> rawTweets = sc.textFile(pathToFile);

        JavaRDD<Tweet> tweets = rawTweets.map(line -> TweetParser.parseJson(line));

        // For each tweet t, we extract all the hashtags and create a pair (hashtag,user)
        JavaPairRDD<String, String> pairs = tweets.flatMapToPair(new HashtagToTweetExtractor());

        // We use the groupBy method to group the users by hashtag
        JavaPairRDD<String, Iterable<String>> tweetsByHashtag = pairs.groupByKey();

        // Then return a map using the collectAsMap method on the RDD
        Map<String, Iterable<String>> map = tweetsByHashtag.collectAsMap();

        for(String hashtag : map.keySet()){
            System.out.println(hashtag + " -> " + map.get(hashtag));
        }

        sc.stop();

    }

    /*A function that returns zero or more key-value pair records from each input record.
    The key-value pairs are represented as Tuple2 objects.
     */
    private static class HashtagToTweetExtractor implements PairFlatMapFunction<Tweet, String, String>{

        @Override
        public Iterator<Tuple2<String, String>> call(Tweet tweet) throws Exception {

            List<Tuple2<String, String>> results = new ArrayList<>();
            List<String> hashtags = new ArrayList<>();
            List<String> words = Arrays.asList(SPACE.split(tweet.getText()));

            for (String word: words) {
                if (word.startsWith("#") && word.length() > 2) {
                    hashtags.add(word);
                }
            }

            for (String hashtag : hashtags) {
                Tuple2<String, String> result = new Tuple2<>(hashtag, tweet.getUser());
                results.add(result);
            }

            return results.iterator();

        }
    }
}

