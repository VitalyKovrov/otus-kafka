package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.example.model.User;
import org.example.serde.ModelSerdes;

import java.time.Duration;

import static org.example.producer.UserProducer.EVENTS_TOPIC;

@Slf4j
public class Main {
    public static void main(String[] args) throws Exception {
        var builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();
        Serde<User> userSerde = ModelSerdes.serde(User.class);

        KTable<Windowed<String>, Long> userUpdatesCount = builder
                .stream(EVENTS_TOPIC, Consumed.with(stringSerde, userSerde))
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(15)))
                .count();
        userUpdatesCount.toStream()
                .foreach((k, v) -> log.info("Window {}: {}", k, v));

        Util.runApp(builder,
                "lesson12",
                1_000_000,
                b -> b.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000));
    }

}
