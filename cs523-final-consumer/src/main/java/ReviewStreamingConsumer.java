import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class ReviewStreamingConsumer {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("KafkaHBaseWordCount").setMaster("local");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Duration.apply(2000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "cs523.final.feedback-publisher");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("sparktest");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaPairDStream<String, String> stringStringJavaPairDStream = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
//        stringStringJavaPairDStream.print();
        stringStringJavaPairDStream.foreachRDD(stringStringJavaPairRDD -> {
            // instantiate Configuration class
            Configuration config = HBaseConfiguration.create();
            Connection conn = ConnectionFactory.createConnection(config);
            Table stream_count = conn.getTable(TableName.valueOf("stream_count"));
            // instantiate Put class
            List<Tuple2<String, String>> collect = stringStringJavaPairRDD.collect();
            collect.forEach(stringStringTuple2 -> {
                Put p = new Put(Bytes.toBytes(1));
                p.addColumn("word".getBytes(),null,stringStringTuple2._2.getBytes());
                try {
                    stream_count.put(p);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        });
        streamingContext.start();
        streamingContext.awaitTermination();
//        JavaReceiverInputDStream<String> localhost = streamingContext.socketTextStream("localhost", 9999,
//                StorageLevel.MEMORY_AND_DISK());
//        JavaDStream<String> stringJavaDStream = localhost.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
//        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream = stringJavaDStream.mapToPair(s -> Tuple2.apply(s, 1));
//        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream1 = stringIntegerJavaPairDStream.reduceByKey(Integer::sum);
//        stringIntegerJavaPairDStream1.print();
//        streamingContext.start();
//        streamingContext.awaitTermination();
    }
}
