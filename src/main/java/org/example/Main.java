package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Properties;

/*


 */

public class Main {
    public static void main(String[] args) throws Exception {
        final String bootstrapServer = "192.168.56.101:9092";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);


        KafkaSource<Tuple2<String, String>> kafkaSource = KafkaSource.<Tuple2<String, String>>builder()
                .setProperties(props)
                .setTopics("closing_price")
                .setDeserializer( new KeyValueDeserializationSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        WatermarkStrategy<Tuple2<String, String>> watermarkStrategy = WatermarkStrategy.<Tuple2<String, String>>forBoundedOutOfOrderness(Duration.ofMillis(200)).withIdleness(Duration.ofSeconds(5));

        DataStreamSource<Tuple2<String, String>> source = env.fromSource(kafkaSource, watermarkStrategy, "kafka source");
        source.print();
        DataStream<Tuple2<String, String>> stream_1 = source.filter((FilterFunction<Tuple2<String, String>>) value -> {
            JSONObject jsonObject = new JSONObject(value.f1);
            return jsonObject.getInt("ASKP1") < 80000;
        });
        stream_1.print();


        KafkaSink<Tuple2<String, String>> kafkaSink = KafkaSink.<Tuple2<String, String>>builder()
                .setBootstrapServers(bootstrapServer)
                .setRecordSerializer(new KeyValueSerializationSchema())
                .build();

        stream_1.sinkTo(kafkaSink);

        // Flink 작업 실행
        env.execute("DataStream Row Field Example");
    }
}