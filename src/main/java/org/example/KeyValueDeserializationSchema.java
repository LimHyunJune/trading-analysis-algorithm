package org.example;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KeyValueDeserializationSchema implements KafkaRecordDeserializationSchema<Tuple2<String,String>> {


    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<>() {
            @Override
            public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                return super.getTypeInfo();
            }
        });
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Tuple2<String, String>> out) {
        String key = record.key() == null ? "null" : new String(record.key());
        String val = record.value() == null ? "null" : new String(record.value());
        out.collect(new Tuple2<>(key, val));
    }
}
