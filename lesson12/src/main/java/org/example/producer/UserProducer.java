package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.Util;
import org.example.model.User;

import java.util.List;

public class UserProducer extends AbstractProducer {
    public static final String EVENTS_TOPIC = "events";
    private final int usersCount;

    public UserProducer(int usersCount) {
        super(EVENTS_TOPIC, Util.producerConfig);
        this.usersCount = usersCount;
    }

    @Override
    protected void doSend(KafkaProducer<String, String> producer) throws Exception {
        int counter = 0;
        List<User> users = Util.generateUsers(usersCount);
        while (counter++ < usersCount && !Thread.interrupted()) {
            User user = users.get(counter);
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    EVENTS_TOPIC, user.getId(), convertToJson(user));
            producer.send(record);
            producer.flush();
            Thread.sleep(1000);
        }
    }
}
