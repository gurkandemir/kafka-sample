package com.learnkafka.libraryeventsconsumer.entity;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Entity
public class LibraryEvent implements Serializable {

    @Id
    @GeneratedValue
    private Integer libraryEventId;

    @Enumerated(EnumType.STRING)
    private LibraryEventType libraryEventType;

    @OneToOne(mappedBy = "libraryEvent", cascade = {CascadeType.ALL})
    @ToString.Exclude
    private Book book;

    public LibraryEventType getLibraryEventType() {
        return libraryEventType;
    }

    public void setLibraryEventType(LibraryEventType libraryEventType) {
        this.libraryEventType = libraryEventType;
    }

    public Integer getLibraryEventId() {
        return libraryEventId;
    }

    public void setLibraryEventId(Integer libraryEventId) {
        this.libraryEventId = libraryEventId;
    }

    public Book getBook() {
        return book;
    }

    public void setBook(Book book) {
        this.book = book;
    }
}
