package org.sparkexample;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.codehaus.jackson.map.util.JSONPObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * WordCountTask class, we will call this class with the test WordCountTest.
 */
public class WordCountTask {
    /**
     * We use a logger to print the output. Sl4j is a common library which works with log4j, the
     * logging system used by Apache Spark.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTask.class);

    /**
     * This is the entry point when the task is called from command line with spark-submit.sh.
     * See {@see http://spark.apache.org/docs/latest/submitting-applications.html}
     */
    public static void main(String[] args) {
//    checkArgument(args.length > 0, "Please provide the path of input file as first parameter.");
        new WordCountTask().run("/testdata/loremipsum.txt");
    }

    /**
     * The task body
     */
    public void run(String inputFilePath) {
    /*
     * This is the address of the Spark cluster. We will call the task from WordCountTest and we
     * use a local standalone cluster. [*] means use all the cores available.
     * See {@see http://spark.apache.org/docs/latest/submitting-applications.html#master-urls}.
     */
        String master = "local";

    /*
     * Initialises a Spark context.
     */
//    SparkConf conf = new SparkConf()
//        .setAppName(WordCountTask.class.getName())
//        .setMaster(master);
//    conf.set("es.index.auto.create", "true");
//    conf.set("es.nodes", "localhost:9200");
//    conf.set("es.clustername", "elasticsearch");
//    conf.set("es.resource","hadoop/contact");

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes
                .LongType, false),DataTypes.createStructField("name",DataTypes.StringType,false),
        DataTypes.createStructField("score",DataTypes.DoubleType,false)});

        SparkSession spark = SparkSession.builder()
                .master(master)
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test.myCollection")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test.myCollection")
                .getOrCreate();
        Dataset<Row> dataSet=spark.read().option("header", true).schema(schema).csv(inputFilePath);

        System.out.println("dataSet.count() = " + dataSet.count());

        JavaSparkContext context = new JavaSparkContext(spark.sparkContext());

        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(context).withOptions(writeOverrides);

    /*
     * Performs a work count sequence of tasks and prints the output with a logger.
     */

//        JavaRDD<Document> sparkDocuments = context.textFile(inputFilePath)
//                .flatMap(text -> Arrays.asList(text.split(" ")).iterator())
//                .mapToPair(word -> new Tuple2<>(word.replaceAll("[-+.^:,]", ""), 1))
//                .reduceByKey((a, b) -> a + b)
//                .map(stringIntegerTuple2 -> {
//                    System.out.println("??????/:::" + "{" + stringIntegerTuple2._1() + ":" + stringIntegerTuple2._2()
//                            + "}");
//                    return Document.parse("{test:" + stringIntegerTuple2._2() + "}");
//
//                });
//
//        MongoSpark.save(sparkDocuments, writeConfig);


    }
}
