package com.db.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${app.kafka.topic:trades}")
    private String tradeTopic;

    @Bean
    public NewTopic tradeTopic() {
        return TopicBuilder.name(tradeTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
