package org.example;

import com.github.javafaker.Faker;
import com.github.javafaker.Name;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.example.model.User;
import org.example.producer.UserProducer;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

@Slf4j
public class Util {

    public static final String HOST = "localhost:9092";

    private static final Map<String, Object> streamsConfig = Map.of(
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);

    public static final Map<String, Object> adminConfig = Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);

    public static final Map<String, Object> producerConfig = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST,
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    public static StreamsConfig createStreamsConfig(Consumer<Map<String, Object>> builder) {
        var map = new HashMap<>(streamsConfig);
        builder.accept(map);
        return new StreamsConfig(map);
    }

    public static void runApp(StreamsBuilder builder, String name, int userCount,
                                   Consumer<Map<String, Object>> configBuilder) throws Exception {
        createTopics(UserProducer.EVENTS_TOPIC);

        var topology = builder.build();

        log.warn("{}", topology.describe());

        try (
                var kafkaStreams = new KafkaStreams(topology, createStreamsConfig(b -> {
                    b.put(StreamsConfig.APPLICATION_ID_CONFIG, name + "-" + UUID.randomUUID().hashCode());
                    b.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

                    configBuilder.accept(b);
                }));
                var producer = new UserProducer(userCount)
        ) {
            log.info("App Started");
            kafkaStreams.start();
            producer.start();

            producer.join();
            Thread.sleep(2000);
            log.info("Shutting down now");
        }
    }

    public static List<User> generateUsers(int count) {
        List<User> users = new ArrayList<>(count);
        List<String> userIds = Arrays.asList("12345678", "222333444", "33311111", "55556666", "4488990011");
        Faker faker = new Faker();
        Random random = new Random();
        for (int i = 0; i < count; i++) {
            Name name = faker.name();
            String userId = userIds.get(random.nextInt(userIds.size()));
            int age = faker.number().numberBetween(18, 90);
            users.add(new User(userId, name.firstName(), name.lastName(), age));
        }
        return users;
    }

    public static void createTopics(String... topics) {
        try (var admin = Admin.create(adminConfig)) {
            admin.createTopics(Stream.of(topics)
                    .map(topic -> new NewTopic(topic, 1, (short) 1))
                    .toList());
        }
    }
}
