package com.learnkafka.libraryeventsproducer.controller;

import com.learnkafka.libraryeventsproducer.domain.Book;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.bootstrap-servers=${spring.embedded.kafka.brokers}"})
class LibraryEventsControllerIT {

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @Test
    @Timeout(5)
    public void post_library_event() {
        // given
        Book book = new Book();
        book.setBookId(123);
        book.setBookAuthor("Gürkan Demir");
        book.setBookName("Learn Kafka");

        LibraryEvent libraryEvent = new LibraryEvent();
        libraryEvent.setBook(book);
        libraryEvent.setLibraryEventId(1);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, httpHeaders);

        // when
        ResponseEntity<LibraryEvent> response = restTemplate.exchange("http://localhost:8080/v1/libraryevent", HttpMethod.POST, request, LibraryEvent.class);

        // then
        ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        String value = record.value();

        assertEquals(response.getStatusCode(), HttpStatus.CREATED);
        assertEquals(value,
                "{\"libraryEventId\":1,\"book\":{\"bookId\":123,\"bookName\":\"Learn Kafka\",\"bookAuthor\":\"Gürkan Demir\"},\"libraryEventType\":\"NEW\"}"
        );
    }

    @Test
    @Timeout(5)
    public void put_library_event() {
        // given
        Book book = new Book();
        book.setBookId(123);
        book.setBookAuthor("Gürkan Demir");
        book.setBookName("Learn Kafka");

        LibraryEvent libraryEvent = new LibraryEvent();
        libraryEvent.setBook(book);
        libraryEvent.setLibraryEventId(1);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, httpHeaders);

        // when
        ResponseEntity<LibraryEvent> response = restTemplate.exchange("http://localhost:8080/v1/libraryevent", HttpMethod.PUT, request, LibraryEvent.class);

        // then
        ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        String value = record.value();

        assertEquals(response.getStatusCode(), HttpStatus.OK);
        assertEquals(value,
                "{\"libraryEventId\":1,\"book\":{\"bookId\":123,\"bookName\":\"Learn Kafka\",\"bookAuthor\":\"Gürkan Demir\"},\"libraryEventType\":\"UPDATE\"}"
        );
    }
}