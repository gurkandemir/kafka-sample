package com.learnkafka.libraryeventsconsumer.entity;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Entity
public class Book implements Serializable {

    @Id
    private Integer bookId;
    private String bookName;
    private String bookAuthor;

    @OneToOne
    @JoinColumn(name = "libraryEventId")
    private LibraryEvent libraryEvent;

    public LibraryEvent getLibraryEvent() {
        return libraryEvent;
    }

    public void setLibraryEvent(LibraryEvent libraryEvent) {
        this.libraryEvent = libraryEvent;
    }

    public Integer getBookId() {
        return bookId;
    }

    public void setBookId(Integer bookId) {
        this.bookId = bookId;
    }

    public String getBookName() {
        return bookName;
    }

    public void setBookName(String bookName) {
        this.bookName = bookName;
    }

    public String getBookAuthor() {
        return bookAuthor;
    }

    public void setBookAuthor(String bookAuthor) {
        this.bookAuthor = bookAuthor;
    }
}
