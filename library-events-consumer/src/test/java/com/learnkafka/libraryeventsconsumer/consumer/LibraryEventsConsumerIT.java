package com.learnkafka.libraryeventsconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsconsumer.entity.Book;
import com.learnkafka.libraryeventsconsumer.entity.LibraryEvent;
import com.learnkafka.libraryeventsconsumer.entity.LibraryEventType;
import com.learnkafka.libraryeventsconsumer.jpa.LibraryEventsRepository;
import com.learnkafka.libraryeventsconsumer.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
class LibraryEventsConsumerIT {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumer;

    @SpyBean
    LibraryEventsService libraryEventsService;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getAllListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    public void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\"libraryEventId\":1,\"book\":{\"bookId\":123,\"bookName\":\"Learn Kafka\",\"bookAuthor\":\"Gürkan Demir\"},\"libraryEventType\":\"NEW\"}";
        kafkaTemplate.sendDefault(json).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> all = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assertEquals(1, all.size());
        all.forEach(event -> {
            assertNotNull(event.getLibraryEventId());
            assertEquals(LibraryEventType.NEW, event.getLibraryEventType());
            assertEquals(123, event.getBook().getBookId());
        });
    }

    @Test
    public void publishUpdateLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\"libraryEventId\":null,\"book\":{\"bookId\":123,\"bookName\":\"Learn Kafka\",\"bookAuthor\":\"Gürkan Demir\"},\"libraryEventType\":\"NEW\"}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = new Book();
        updatedBook.setBookId(123);
        updatedBook.setBookAuthor("Cansu Dogan");
        updatedBook.setBookName("KFL 2015");
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);

        String updatedJson = objectMapper.writeValueAsString(libraryEvent);

        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> all = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assertEquals(1, all.size());
        all.forEach(event -> {
            assertNotNull(event.getLibraryEventId());
            assertEquals(LibraryEventType.UPDATE, event.getLibraryEventType());
            assertEquals(1, event.getLibraryEventId());
            assertEquals(123, event.getBook().getBookId());
            assertEquals("Cansu Dogan", event.getBook().getBookAuthor());
            assertEquals("KFL 2015", event.getBook().getBookName());
        });
    }

    @Test
    public void shouldNotPublishUpdateLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\"libraryEventId\":1,\"book\":{\"bookId\":123,\"bookName\":\"Learn Kafka\",\"bookAuthor\":\"Gürkan Demir\"},\"libraryEventType\":\"UPDATE\"}";
        kafkaTemplate.sendDefault(1, json).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumer, atLeast(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsService, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));

        Optional<LibraryEvent> event = libraryEventsRepository.findById(1);
        assertFalse(event.isPresent());
    }

    @Test
    public void shouldNotPublishNullUpdateLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\"libraryEventId\":null,\"book\":{\"bookId\":123,\"bookName\":\"Learn Kafka\",\"bookAuthor\":\"Gürkan Demir\"},\"libraryEventType\":\"UPDATE\"}";
        kafkaTemplate.sendDefault(null, json).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
    }

    @Test
    public void shouldNotPublishButRecoverNullUpdateLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        Integer id = 000;
        String json = "{\"libraryEventId\":" + id + ",\"book\":{\"bookId\":123,\"bookName\":\"Learn Kafka\",\"bookAuthor\":\"Gürkan Demir\"},\"libraryEventType\":\"UPDATE\"}";
        kafkaTemplate.sendDefault(id, json).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsService, times(1)).handleRecovery(isA(ConsumerRecord.class));
    }
}