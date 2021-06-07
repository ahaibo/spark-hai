package com.hai.bgdata.spark.java.wc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * description
 *
 * @author hai
 * @date 2021/6/5 16:44
 */
public class JavaWordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaWordCount3");

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> lines = context.textFile("data/input/data*.txt");

        JavaRDD<String> flatMapRDD = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        // JavaPairRDD<String, Integer> objectObjectJavaPairRDD = lines.flatMapToPair(s -> {
        //     Iterator<String> iterator = Arrays.asList(s.split(" ")).iterator();
        //     while (iterator.hasNext()) {
        //         new Tuple2<>(iterator.next(), 1);
        //     }
        // });

        JavaPairRDD<String, Integer> pairRDD = flatMapRDD.mapToPair(iter -> new Tuple2<>(iter, 1));

        JavaPairRDD<String, Integer> reduceByKeyRDD = pairRDD.reduceByKey((v1, v2) -> v1 + v2);

        // JavaRDD<Tuple2> map = reduceByKeyRDD.map(t -> new Tuple2(t._2, t._1));
        // JavaRDD<Tuple2<Integer, String>> swapTuple = reduceByKeyRDD.map(t -> t.swap());

        //java rdd sort
        // JavaPairRDD<Integer, String> swapPairRDD = reduceByKeyRDD.mapToPair(t -> new Tuple2<>(t._2, t._1));
        JavaPairRDD<Integer, String> swapPairRDD = reduceByKeyRDD.mapToPair(t -> t.swap());
        JavaPairRDD<Integer, String> sortByKeyRDD = swapPairRDD.sortByKey(false, 1);//排序

        // sortByKeyRDD.map(t -> t.swap()).foreach(i -> System.out.println(i));
        sortByKeyRDD.mapToPair(t -> t.swap()).foreach(i -> System.out.println(i));

        context.stop();
    }
}
