package a.em.log4j2.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducerManager {

    private final String topic;
    private final Producer<String, String> producer;

    public KafkaProducerManager(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        this.producer = new KafkaProducer<>(props);
    }

    public void send(String message) {
        producer.send(new ProducerRecord<>(topic, message));
    }

    public void close(long timeout, TimeUnit timeUnit) {
        producer.close(Duration.of(timeout, timeUnit.toChronoUnit()));

    }
}
