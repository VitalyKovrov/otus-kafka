package org.example;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Getter
public abstract class AbstractConsumer implements AutoCloseable {
    protected final Thread thread = new Thread(this::process);
    private final List<String> topics;
    protected final Map<String, Object> config;
    private final String name;

    public AbstractConsumer(String name, List<String> topics, Map<String, Object> config) {
        this.topics = topics;
        this.name = name;
        this.config = new HashMap<>(config);
        thread.setName("MessageConsumer.".concat(name));
        thread.setDaemon(true);
    }

    private void process() {
        log.info(thread.getName().concat(" started."));
        try (var consumer = new KafkaConsumer<String, String>(config)) {
            consumer.subscribe(topics);
            while (!Thread.interrupted()) {
                var read = consumer.poll(Duration.ofSeconds(1));
                for (var record : read) {
                    processRecord(record);
                }
            }
        } catch (Exception ignored) {

        }
        log.info(thread.getName().concat(" stopped."));
    }

    protected abstract void processRecord(ConsumerRecord<String, String> record);


    @Override
    public void close() throws Exception {
        thread.interrupt();
        thread.join();
    }
}
