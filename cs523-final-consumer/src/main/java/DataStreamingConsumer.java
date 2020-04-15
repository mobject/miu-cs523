import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

public class DataStreamingConsumer {

    public static final String TABLE_NAME = "NEOWS_DATA";
    public static final String kAFKA_TOPIC_NAME = "cs532";

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("KafkaHBaseWordCount").setMaster("local");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Duration.apply(1000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", NeoWSDataDeserializer.class);
        kafkaParams.put("group.id", "cs523.final.neows");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList(kAFKA_TOPIC_NAME);

        JavaInputDStream<ConsumerRecord<String, NeoWSData>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferBrokers(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        JavaPairDStream<String, NeoWSData> stringStringJavaPairDStream = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        stringStringJavaPairDStream.foreachRDD(stringStringJavaPairRDD -> {
            // instantiate Configuration class
            Configuration config = HBaseConfiguration.create();
            Connection conn = ConnectionFactory.createConnection(config);
            Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
            // instantiate Put class
            List<Tuple2<String, NeoWSData>> collect = stringStringJavaPairRDD.collect();
            final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            collect.forEach(stringStringTuple2 -> {
                NeoWSData neoWSData = stringStringTuple2._2;
                Map<String, Object> near_earth_objects = neoWSData.getNear_earth_objects();
                String next = near_earth_objects.keySet().iterator().next();
                ArrayList<LinkedHashMap> props = (ArrayList)near_earth_objects.get(next);
                props.toString();
                props.forEach(o -> {
//                    info', VERSIONS=> 5} ,{NAME => 'size', VERSIONS=> 5}, {NAME => 'event'
                    String id = o.get("id").toString();
                    String name = o.get("name").toString();
                    LinkedHashMap estimated_diameter = (LinkedHashMap)o.get("estimated_diameter");
                    LinkedHashMap meters = (LinkedHashMap)estimated_diameter.get("meters");

                    ArrayList close_approach_data = (ArrayList)o.get("close_approach_data");
                    LinkedHashMap relative_velocity = (LinkedHashMap)((LinkedHashMap)close_approach_data.get(0)).get("relative_velocity");

                    LinkedHashMap miss_distance = (LinkedHashMap)((LinkedHashMap)close_approach_data.get(0)).get("miss_distance");
                    Put p = new Put(id.getBytes());
                    p.addColumn("info".getBytes(), "name".getBytes(), name.getBytes());
                    double diameter_max = Double.parseDouble(meters.get("estimated_diameter_max").toString());
                    p.addColumn("size".getBytes(),"estimated_diameter_meter".getBytes(),doubleToByteArray(diameter_max));

                    p.addColumn("event".getBytes(), "kilometers_per_second".getBytes(), doubleToByteArray(Double.parseDouble(relative_velocity.get("kilometers_per_second").toString())));
                    p.addColumn("event".getBytes(), "missed_distance_miles".getBytes(), doubleToByteArray(Double.parseDouble(miss_distance.get("kilometers").toString())));

                    try {
                        Date date = simpleDateFormat.parse(next);
                        byte[] bytes = PDate.INSTANCE.toBytes(date);
                        p.addColumn("event".getBytes(), "close_approach_date".getBytes(), bytes);
                        table.put(p);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });


            });
        });
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private static byte[] doubleToByteArray ( final double i ){
        return PDouble.INSTANCE.toBytes(i);
    }
}
