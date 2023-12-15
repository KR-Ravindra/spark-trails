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

public class CombineUni {
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


        JavaRDD<String> studentRDD = sparkContext.textFile("/Users/kr-ravindra/Downloads/SPARK/studentAnalysis/input/student-uni.txt",1);
        JavaRDD<String> universityRDD = sparkContext.textFile("/Users/kr-ravindra/Downloads/SPARK/studentAnalysis/input/university-pro.txt",1);


        JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> studentUniProgramRDD = studentRDD
        .mapToPair(new PairFunction<String, Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<Tuple2<String, String>, Tuple2<String, String>> call(String s) {
                String[] parts = s.split(" ");
                return new Tuple2<>(new Tuple2<>(parts[2], parts[3]), new Tuple2<>(parts[0], parts[1]));
            }
        });

        JavaPairRDD<Tuple2<String, String>, String> universityProgramRDD = universityRDD
        .mapToPair(new PairFunction<String, Tuple2<String, String>, String>() {
            @Override
            public Tuple2<Tuple2<String, String>, String> call(String s) {
                String[] parts = s.split(" ");
                return new Tuple2<>(new Tuple2<>(parts[0], parts[1]), parts[2]);
            }
        });

        JavaPairRDD<Tuple2<String, String>, Tuple2<Tuple2<String, String>, String>> joinedRDD = studentUniProgramRDD.join(universityProgramRDD);
        JavaRDD<String> finalRDD = joinedRDD.map(new Function<Tuple2<Tuple2<String, String>, Tuple2<Tuple2<String, String>, String>>, String>() {
            @Override
            public String call(Tuple2<Tuple2<String, String>, Tuple2<Tuple2<String, String>, String>> record) {
                Tuple2<String, String> uniProgram = record._1;
                Tuple2<String, String> name = record._2._1;
                String units = record._2._2;
                return name._1 + " " + name._2 + " " + uniProgram._1 + " " + uniProgram._2 + " " + units;
            }
        });
        
        finalRDD.saveAsTextFile("/Users/kr-ravindra/Downloads/SPARK/studentAnalysis/output/finalRDD");
        joinedRDD.saveAsTextFile("/Users/kr-ravindra/Downloads/SPARK/studentAnalysis/output/joinedRDD");
        universityProgramRDD.saveAsTextFile("/Users/kr-ravindra/Downloads/SPARK/studentAnalysis/output/universityProgramRDD");
        studentUniProgramRDD.saveAsTextFile("/Users/kr-ravindra/Downloads/SPARK/studentAnalysis/output/studentUniProgramRDDKV");
        studentRDD.saveAsTextFile("/Users/kr-ravindra/Downloads/SPARK/studentAnalysis/output/studentUniProgramRDD");


    }
}
