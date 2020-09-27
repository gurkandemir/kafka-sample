# Library Events Kafka Producer - Consumer
Sample producer and consumer using Apache Kafka with Java Springboot.

## Endpoints

There are two endpoints which are used for creating new library event, and updating existing one.

- Creating library event
  ```
  curl --location --request POST 'localhost:8080/v1/libraryevent' \
       --header 'Content-Type: application/json' \
       --data-raw '{
           "book": {
              "bookId": 1,
              "bookName": "Beyaz Dis",
              "bookAuthor": "Jack London"
            }
        }'
  ```
  
  Kafka produces a message to queue in order to consume later.
  
  *```Message sent successfully for the key: null and the value:{"libraryEventId":null,"book":{"bookId":1,"bookName":"Beyaz Dis","bookAuthor":"Jack London"},"libraryEventType":"NEW"} partition: 2```*

  
- Updating library event
  ```
  curl --location --request PUT 'localhost:8080/v1/libraryevent' \
       --header 'Content-Type: application/json' \
       --data-raw '{
           "libraryEventId": 1,
           "book": {
              "bookId": 1,
              "bookName": "Beyaz Dis 2",
              "bookAuthor": "Jack London"
            }
       }'
  ```
  
    Kafka produces a message to queue in order to consume later.
  
  *```Message sent successfully for the key: 1 and the value:{"libraryEventId":1,"book":{"bookId":1,"bookName":"Beyaz Dis 2","bookAuthor":"Jack London"},"libraryEventType":"UPDATE"} partition: 0```*

## Notes
 - Producer starts on port **8080**.
 - Consumer starts on port **8082**.
 - There exists only one Kafka broker which starts on port **9092**.
 - There exist *error handling, retry - recovery mechanisms* in both consumer and producer.
 
## About Project
  - Both projects are written with JAVA11.
  - Both of them are maven project.
