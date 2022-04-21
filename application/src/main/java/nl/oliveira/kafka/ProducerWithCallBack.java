package nl.oliveira.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class ProducerWithCallBack {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallBack.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Producing message");
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 3000; i++) {
            final ProducerRecord<String, String> record = new ProducerRecord<>("first_java_topic", "hello " + i);

            producer.send(record, (metadata, e) -> {
                if (e == null) {
                    log.info("Received new metadata/ \n"
                            + "Topic: " + metadata.topic() + "\n"
                            + "Partition: " + metadata.partition() + "\n"
                            + "Offset: " + metadata.offset() + "\n"
                            + "Timestamp: " + metadata.timestamp() + "\n");
                } else {
                    log.error("Error while producing: ", e);
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
