package org.example;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.function.Consumer;

@Slf4j
public class Main {

    @SneakyThrows
    public static void main(String[] args) {
        Util.createTopics("topic1", "topic2");

        try (
                var producer = new KafkaProducer<String, String>(
                        Util.createProducerConfig((config) -> config.put(
                                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "ex7")));
                var consumer = new LoggingConsumer("ReadCommitted", List.of("topic1", "topic2"), Util.consumerConfig, true)) {


            Consumer<Integer> sendMessages = (Integer count) -> {
                for (int i = 1; i <= count; i++) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    producer.send(new ProducerRecord<>("topic1", "key", String.valueOf(i)));
                    producer.send(new ProducerRecord<>("topic2", "key", String.valueOf(i)));
                }
            };

            producer.initTransactions();

            log.info("beginTransaction");
            producer.beginTransaction();
            sendMessages.accept(5);
            log.info("commitTransaction");
            producer.commitTransaction();

            producer.beginTransaction();
            sendMessages.accept(2);
            log.info("abortTransaction");
            producer.abortTransaction();

            Thread.sleep(1000);
        }
    }
}