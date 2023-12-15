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

public class CalculateAvgGPA {
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

        // Loading the Student & Course datasets
        JavaRDD<String> studentRDD = sparkContext.textFile(args[0],4);
        JavaRDD<String> courseRDD = sparkContext.textFile(args[1], 6);

        // Filter out the famale students
        JavaRDD<String> femaleStudentRDD = studentRDD.filter(
                new Function<String, Boolean>() {
                    public Boolean call(String s) throws Exception {
                        String gender = s.split(" ")[3];
                        return gender.equalsIgnoreCase("female");
                    }
                }
        );

        JavaRDD<String> mappedStudentRDD = studentRDD.map(
                new Function<String, String>() {
                    public String call(String s) throws Exception {
                        String outStr = s.split(" ")[0] + " " + s.split(" ")[1] + " " + s.split(" ")[2];
                        return outStr;
                    }
                }
        );

        // Convert both RDD into keyvalue Pair RDD
        JavaPairRDD<String, String> studentPairRDD = femaleStudentRDD.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String s) throws Exception {
                        String cwid = s.split(" ")[0];
                        return new Tuple2(cwid, s);
                    }
                }
        );
        JavaPairRDD<String, String> coursePairRDD = courseRDD.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String s) throws Exception {
                        String cwid = s.split(" ")[0];
                        return new Tuple2(cwid, s);
                    }
                }
        );
        // Perform the Join operation (Wide Transform)
        JavaPairRDD<String, Tuple2<String, String>> joinPairRDD = studentPairRDD.join(coursePairRDD, 4);

        JavaRDD<Tuple2<String, Tuple2<Float, Integer>>> gpaCntRDD = joinPairRDD.map(
                new Function<Tuple2<String, Tuple2<String, String>>, Tuple2<String, Tuple2<Float, Integer>>>() {
                    public Tuple2<String, Tuple2<Float, Integer>> call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
                        String cwid = stringTuple2Tuple2._1;
                        String course = stringTuple2Tuple2._2._2;
                        Float gpa = new Float(course.split(" ")[2]);
                        return new Tuple2(cwid, new Tuple2(gpa, 1));
                    }
                }
        );

        JavaPairRDD<String, Tuple2<Float, Integer>> gpaCntPairRDD = gpaCntRDD.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<Float, Integer>>, String, Tuple2<Float, Integer>>() {
                    public Tuple2<String, Tuple2<Float, Integer>> call(Tuple2<String, Tuple2<Float, Integer>> stringTuple2Tuple2) throws Exception {
                        return stringTuple2Tuple2;
                    }
                }
        );

        JavaPairRDD sumGpaCntPairRDD = gpaCntPairRDD.reduceByKey(
                new Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>>() {
                    public Tuple2<Float, Integer> call(Tuple2<Float, Integer> floatIntegerTuple2, Tuple2<Float, Integer> floatIntegerTuple22) throws Exception {
                        Float sumGpa = floatIntegerTuple2._1 + floatIntegerTuple22._1;
                        Integer sumCnt = floatIntegerTuple2._2 + floatIntegerTuple22._2;
                        return new Tuple2(sumGpa, sumCnt);
                    }
                }
        );

        JavaPairRDD<Tuple2<String, Integer>, String> genderLevelPairRDD = studentRDD.mapToPair(
                new PairFunction<String, Tuple2<String, Integer>, String>() {
                    public Tuple2<Tuple2<String, Integer>, String> call(String s) throws Exception {
                        String gender = s.split(" ")[3];
                        Integer level = new Integer(s.split(" ")[4]);
                        return new Tuple2(new Tuple2(gender, level), s);
                    }
                }
        );

        JavaPairRDD sortedRDD = genderLevelPairRDD.sortByKey(new TupleKeyComparator());

        //
        studentRDD.saveAsTextFile("/Users/kr-ravindra/Downloads/SPARK/studentAnalysis/output/studentRDD");
        femaleStudentRDD.saveAsTextFile("/Users/kr-ravindra/Downloads/SPARK/studentAnalysis/output/femaleStudentRDD");
        mappedStudentRDD.saveAsTextFile("/Users/kr-ravindra/Downloads/SPARK/studentAnalysis/output/mappedStudentRDD");
        sortedRDD.saveAsTextFile("/Users/kr-ravindra/Downloads/SPARK/studentAnalysis/output/sortedRDD");
    }
}
