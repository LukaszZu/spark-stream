/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.sparktest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

/**
 *
 * @author zulk
 */
public class SParkTest {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws InterruptedException {
        // TODO code application logic here
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount");
//        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        
         Set<String> topicsSet = new HashSet<>(Arrays.asList("test_multi".split(",")));
    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("zookeeper.connect", "192.168.100.105:2181");
    kafkaParams.put("metadata.broker.list", "192.168.100.105:9092");
    

    // Create direct kafka stream with brokers and topics
    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
        jsc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        kafkaParams,
        topicsSet
    );
        messages.foreachRDD(rdd -> {
            rdd.foreachPartition(r -> {
                        ObjectMapper objectMapper = Mapper.of();
                        System.out.println(Thread.currentThread().getName());
                        r.forEachRemaining(p -> {
                            process(p,objectMapper);
                        });
                    }
            );
        });
        jsc.start();
        jsc.awaitTermination();
    
    }

    private static void process(Tuple2<String, String> p,ObjectMapper objectMapper) {
        try {
            Class<?> forName = Class.forName("com.mycompany.sparktest.Test");
            System.out.println(p._1+"|"+objectMapper.readValue(p._2, forName));
        } catch (IOException|ClassNotFoundException ex) {
            Logger.getLogger(SParkTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
