package com.learnkafka.libraryeventsconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsconsumer.entity.LibraryEvent;
import com.learnkafka.libraryeventsconsumer.entity.LibraryEventType;
import com.learnkafka.libraryeventsconsumer.jpa.LibraryEventsRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class LibraryEventsService {

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);

        if (libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 000) {
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }

        if (libraryEvent.getLibraryEventType().equals(LibraryEventType.NEW)) {
            save(libraryEvent);
        } else if (libraryEvent.getLibraryEventType().equals(LibraryEventType.UPDATE)) {
            update(libraryEvent);
        } else {
            System.out.println("Invalid type!");
        }
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
    }

    private void update(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null)
            throw new IllegalArgumentException("Library event is null");

        libraryEventsRepository.findById(libraryEvent.getLibraryEventId())
                .ifPresentOrElse(
                        updated -> {
                            updated.setLibraryEventType(LibraryEventType.UPDATE);
                            updated.getBook().setBookAuthor(libraryEvent.getBook().getBookAuthor());
                            updated.getBook().setBookName(libraryEvent.getBook().getBookName());
                            updated.getBook().setBookId(libraryEvent.getBook().getBookId());
                            libraryEventsRepository.save(updated);
                        }, () -> {
                            throw new IllegalArgumentException("Not found library event");
                        });
    }

    public void handleRecovery(ConsumerRecord<Integer, String> record) {
        Integer key = record.key();
        String value = record.value();

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        System.out.println("Message sent successfully for the key: " + key + " and the value:" + value + " partition: " + result.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        System.out.println("Error while sending the message, ex: " + ex.getMessage());
        try {
            throw ex;
        } catch (Throwable throwable) {
            System.out.println("Error on failure: " + throwable.getMessage());
        }
    }

}
