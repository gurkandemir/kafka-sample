package com.learnkafka.libraryeventsconsumer.config;

import com.learnkafka.libraryeventsconsumer.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

    @Autowired
    LibraryEventsService libraryEventsService;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory.getObject());
        factory.setConcurrency(3);
        factory.setErrorHandler(((thrownException, data) -> {
            System.out.println(thrownException.getMessage());
            System.out.println(data);
        }));

        factory.setRetryTemplate(retryTemplate());
        factory.setRecoveryCallback(context -> {
            if (context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
                System.out.println("Inside recoverable logic");
                ConsumerRecord<Integer, String> record = (ConsumerRecord<Integer, String>) context.getAttribute("record");
                libraryEventsService.handleRecovery(record);
            } else {
                System.out.println("Inside non recoverable logic");
                throw new RuntimeException(context.getLastThrowable().getMessage());
            }

            return null;
        });
        return factory;
    }

    private RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy());
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy());
        return retryTemplate;
    }

    private RetryPolicy simpleRetryPolicy() {
       /* SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        simpleRetryPolicy.setMaxAttempts(3);
        */

        Map<Class<? extends Throwable>, Boolean> exceptionsMaps = new HashMap<>();
        exceptionsMaps.put(RecoverableDataAccessException.class, true);
        exceptionsMaps.put(IllegalArgumentException.class, false);

        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, exceptionsMaps, true);
        return simpleRetryPolicy;
    }

    private FixedBackOffPolicy fixedBackOffPolicy() {
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000);
        return fixedBackOffPolicy;
    }
}
