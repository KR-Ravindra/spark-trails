import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class CalculateBmi {
    public static void main(String[] args) {
        // Create the Spark application
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]");  // Delete this line when submitting to a cluster
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //System.out.println("The parallelism : " + sparkContext.defaultParallelism());

        Logger log = LogManager.getRootLogger();
        log.setLevel(Level.DEBUG);
        log.debug("The parallelism : " + sparkContext.defaultParallelism());


        JavaRDD<String> peopleRDD = sparkContext.textFile(args[0],1);

        JavaRDD<String> mappedPeopleRDD = peopleRDD.map(
            new Function<String, String>() {
                public String call(String s) throws Exception {
                    String[] splitValues = s.split(" ");
                    double weight = Double.parseDouble(splitValues[3]);
                    double height = Double.parseDouble(splitValues[2]);
                    String outStr = splitValues[0] + " " + splitValues[1] + " " + (weight * 703) / (height * 144 * height);
                    return outStr;
                }
            }
        );

        JavaRDD<String> overWeightPeopleRDD = mappedPeopleRDD.filter(
            new Function<String, Boolean>() {
                public Boolean call(String s) throws Exception {
                    double bmi = Double.parseDouble(s.split(" ")[2]);
                    return bmi > 25;
                }
            }
        );
        overWeightPeopleRDD.saveAsTextFile("/Users/kr-ravindra/Downloads/SPARK/studentAnalysis/output/overweightRDD");


    }
}
