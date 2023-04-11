FROM openjdk:8-jdk-alpine
MAINTAINER piyali kamra
COPY target/kafkaproducer-0.0.1-SNAPSHOT.jar kafka-producer.jar
ENTRYPOINT ["java","-jar","/kafka-producer.jar"]