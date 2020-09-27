package com.learnkafka.libraryeventsconsumer.jpa;

import com.learnkafka.libraryeventsconsumer.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

public interface LibraryEventsRepository extends CrudRepository<LibraryEvent, Integer> {
}
