package net.erlantz;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

public class Producer {
    public static void main(String[] args) {
        Properties properties = getProperties();


        final String[] keyList = {"persona", "animal", "cosa"};
        final String[] valueList = {
                "Juan",
                "Maria",
                "Pedro",
                "Ana",
                "Luis",
                "Marta",
                "Jose",
                "Carmen",
                "David",
                "Elena"
        };
        final var rand = new Random();

//        ProduceSync(properties, keyList, rand, valueList);
        ProduceAsync(properties, keyList, rand, valueList);

    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "erlantz.eu:39090,erlantz.eu:39091,erlantz.eu:39092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "3");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "10");
//        properties.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");
        return properties;
    }

    private static void ProduceSync(Properties properties, String[] keyList, Random rand, String[] valueList) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            while (true) {
                for (var key : keyList) {
                    var numerOfMessages = rand.nextInt(10) + 1;
                    System.out.printf("Sending %s messages to %s%n", numerOfMessages, key);
                    for (int i = 0; i < numerOfMessages; i++) {
                        var j = rand.nextInt(valueList.length);
                        var value = valueList[j];

                        System.out.printf("Sending %s->%s  -  ", key, value);
                        var result = producer.send(new ProducerRecord<>("rafa_pons", key, value));
                        System.out.println(result.get().toString());
                    }
                }
                Thread.sleep(Duration.ofMillis(100));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void ProduceAsync(Properties properties, String[] keyList, Random rand, String[] valueList) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            while (true) {
                var results = new ArrayList<Future<RecordMetadata>>();
                for (var key : keyList) {
                    var numerOfMessages = rand.nextInt(10) + 1;
                    System.out.printf("Sending %s messages to %s%n", numerOfMessages, key);
                    for (int i = 0; i < numerOfMessages; i++) {
                        var j = rand.nextInt(valueList.length);
                        var value = valueList[j];

                        System.out.printf("Sending %s->%s%n", key, value);
                        results.add(producer.send(new ProducerRecord<>("rafa_pons", key, value)));
                    }
                }
                for (var result : results) {
                    result.get();
                }
                Thread.sleep(Duration.ofMillis(100));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}