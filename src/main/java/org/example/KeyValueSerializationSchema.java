package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class KeyValueSerializationSchema implements KafkaRecordSerializationSchema<Tuple2<String,String>> {

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, String> element, KafkaSinkContext context, Long timestamp) {
        byte[] key = element.f0.getBytes();
        byte[] value = element.f1.getBytes();
        return new ProducerRecord<>("closing_price_result",key,value);
    }
}
