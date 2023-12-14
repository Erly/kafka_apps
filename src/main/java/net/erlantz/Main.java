package net.erlantz;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class Main {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "erlantz.eu:39090,erlantz.eu:39091,erlantz.eu:39092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final String[] keyList = {"persona", "animal", "cosa"};
        final String[] valueList = {
                "Juan",
                "María",
                "Pedro",
                "Ana",
                "Luis",
                "Marta",
                "José",
                "Carmen",
                "David",
                "Elena"
        };
        final var rand = new Random();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            while (true) {
                for (var key : keyList) {
                    var numerOfMessages = rand.nextInt(5) + 1;
                    System.out.printf("Sending %s messages to %s%n", numerOfMessages, key);
                    for (int i = 0; i < numerOfMessages; i++) {
                        var j = rand.nextInt(valueList.length);
                        var value = valueList[j];

                        System.out.printf("Sending %s->%s  -  ", key, value);
                        var result = producer.send(new ProducerRecord<>("rafa_pons", key, value));
                        System.out.println(result.get().toString());
                    }
                }
                Thread.sleep(1000);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}