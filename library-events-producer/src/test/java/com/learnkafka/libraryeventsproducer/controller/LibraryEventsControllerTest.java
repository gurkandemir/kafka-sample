package com.learnkafka.libraryeventsproducer.controller;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsproducer.domain.Book;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import com.learnkafka.libraryeventsproducer.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
class LibraryEventsControllerTest {

    @Autowired
    MockMvc mockMvc;

    @MockBean
    LibraryEventProducer libraryEventProducer;

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void should_post_library_event() throws Exception {
        // given
        Book book = new Book();
        book.setBookId(123);
        book.setBookAuthor("Gürkan Demir");
        book.setBookName("Learn Kafka");

        LibraryEvent libraryEvent = new LibraryEvent();
        libraryEvent.setBook(book);
        libraryEvent.setLibraryEventId(null);

        String json = objectMapper.writeValueAsString(libraryEvent);
        doNothing().when(libraryEventProducer).sendLibraryEventApproach2(isA(LibraryEvent.class));

        // when
        mockMvc.perform(post("/v1/libraryevent")
                .contentType(MediaType.APPLICATION_JSON)
                .content(json))
                .andExpect(status().isCreated());

        // then
    }

    @Test
    void should_not_put_library_event() throws Exception {
        // given
        Book book = new Book();
        book.setBookId(123);
        book.setBookAuthor("Gürkan Demir");
        book.setBookName("Learn Kafka");

        LibraryEvent libraryEvent = new LibraryEvent();
        libraryEvent.setBook(book);
        libraryEvent.setLibraryEventId(null);

        String json = objectMapper.writeValueAsString(libraryEvent);
        doNothing().when(libraryEventProducer).sendLibraryEventApproach2(isA(LibraryEvent.class));

        // when
        mockMvc.perform(put("/v1/libraryevent")
                .contentType(MediaType.APPLICATION_JSON)
                .content(json))
                .andExpect(status().isBadRequest());

        // then
    }

    @Test
    void should_put_library_event() throws Exception {
        // given
        Book book = new Book();
        book.setBookId(123);
        book.setBookAuthor("Gürkan Demir");
        book.setBookName("Learn Kafka");

        LibraryEvent libraryEvent = new LibraryEvent();
        libraryEvent.setBook(book);
        libraryEvent.setLibraryEventId(1);

        String json = objectMapper.writeValueAsString(libraryEvent);
        doNothing().when(libraryEventProducer).sendLibraryEventApproach2(isA(LibraryEvent.class));

        // when
        mockMvc.perform(put("/v1/libraryevent")
                .contentType(MediaType.APPLICATION_JSON)
                .content(json))
                .andExpect(status().isOk());

        // then
    }
}