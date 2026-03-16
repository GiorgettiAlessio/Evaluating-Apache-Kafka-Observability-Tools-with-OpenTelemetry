package org.example;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingConsumerInterceptor;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.kafka.clients.consumer.*;
import io.opentelemetry.api.trace.Span;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.lang.reflect.Array;
import java.util.*;

public class SampleConsumer implements Runnable {

    private String nome;
    private String topic;
    int numMess;
    private static final Tracer tracer = GlobalOpenTelemetry.getTracer("TracingProducer");


    public SampleConsumer(String n, String t, int num){
        nome=n;
        topic=t;
        numMess= num + 400;
    }


    public void run() {

        Properties properties = new Properties();
        Properties roducerProperties = new Properties();

        if(nome.compareTo("C2")==0){
            properties.put("bootstrap.servers", "192.168.178.119:9094");
            roducerProperties.put("bootstrap.servers", "192.168.178.119:9094");

        }
        else if(nome.compareTo("C5")==0){
            properties.put("bootstrap.servers", "192.168.178.119:9094");
            roducerProperties.put("bootstrap.servers", "192.168.178.119:9094");
        }
        else if(nome.compareTo("C3")==0){
            properties.put("bootstrap.servers", "192.168.178.119:9093");
            roducerProperties.put("bootstrap.servers", "192.168.178.119:9093");
        }
        else{
            properties.put("bootstrap.servers", "192.168.178.119:9092");
            roducerProperties.put("bootstrap.servers", "192.168.178.119:9092");
        }


        properties.put("replication.factor", "3");
        properties.put("partitions", "3");
        properties.put("group.id", "prova_group" + nome + System.currentTimeMillis());
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingConsumerInterceptor.class.getName());


        roducerProperties.put("bootstrap.servers", "192.168.178.119:9092");
        roducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        roducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        roducerProperties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingProducerInterceptor.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(roducerProperties);


        List<String> daScrivere2 = new ArrayList<String>();
        List<String> daScrivere3 = new ArrayList<String>();


        int i = 0;
        boolean spegni = false;



        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties);) {

            consumer.subscribe(Arrays.asList(topic));
            consumer.seekToBeginning(consumer.assignment());

            while (true) {

                if(spegni){


                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        consumer.close();
                        producer.close();
                    }));
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    System.out.println("spegni " + nome);
                    while (true){

                    }
                }

                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {

                    if(i == 0 || (!daScrivere2.isEmpty() && !daScrivere2.contains(record.value())) || (!daScrivere3.isEmpty() && !daScrivere3.contains(record.value()))) {
                        i++;

                        Context extractedContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator().extract(Context.current(), record.headers(), org.example.consumer.HeadersTextMapGetter.INSTANCE);

                        Span span = null;
                        String spanName = "process-message";

                        if (nome.equals("C5") || nome.equals("C2")) {
                            spanName = "consume-message-" + record.value().hashCode();  // Usa un identificatore unico
                            span = tracer.spanBuilder(spanName).setParent(extractedContext)
                                    .setAttribute("messaging.destination.name", record.topic())
                                    .setAttribute("messaging.operation", "process").startSpan();

                            daScrivere2.add(record.value());

                            try (Scope scope = span.makeCurrent()) {
                                System.out.println(nome + "- letto: " + record.value() + " offset: " + record.offset() + " partition: "+ record.partition());
                            } catch (Exception e) {
                                span.recordException(e);
                                throw e;
                            }finally {
                                span.end();
                            }
                        } else {
                            span = tracer.spanBuilder("forward-to-new-topic").setParent(extractedContext).startSpan();

                            try (Scope scope = span.makeCurrent()) {
                                System.out.println(nome + "- letto: " + record.value() + " offset: " + record.offset() + " partition: "+ record.partition());

                                if (record.value().endsWith("-3")) {
                                    daScrivere3.add(record.value());
                                    String newMessage = nome.equals("C1") ? "Upload: " + record.value() : record.value();

                                    if (nome.equals("C4") && record.value().contains("Upload:")) {
                                        sendToTopics(producer, newMessage, "topic5", "topic3");
                                    } else if (nome.equals("C3")) {
                                        sendToTopic(producer, newMessage, "topic4");
                                    } else if (nome.equals("C1")) {
                                        sendToTopic(producer, newMessage, "topic3");
                                    }
                                } else {
                                    daScrivere2.add(record.value());
                                    String newMessage = "Lettura: " + record.value();
                                    sendToTopic(producer, newMessage, "topic2");
                                }
                            } catch (Exception e) {
                                span.recordException(e);
                                throw e;
                            }finally {
                                span.end();
                            }
                        }
                    }


                }



                if(nome.compareTo("C2")==0 || nome.compareTo("C5")==0){
                    if(!daScrivere2.isEmpty() && (daScrivere2.size() == numMess)){
                        spegni = true;
                    }
                }
                else if(nome.compareTo("C1")==0){
                    if(!daScrivere2.isEmpty() && daScrivere2.size()==numMess && !daScrivere3.isEmpty() && daScrivere3.size()==numMess){
                        spegni = true;
                    }
                }
                else if(nome.compareTo("C3")==0 || nome.compareTo("C4")==0){
                    if(!daScrivere3.isEmpty() && daScrivere3.size()==numMess){
                        spegni = true;
                    }
                }



            }

        }
    }

    // Metodo per inviare messaggi a più topic
    private void sendToTopics(KafkaProducer<String, String> producer, String message, String... topics) {
        for (String topic : topics) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), record.headers(), org.example.producer.HeadersTextMapSetter.INSTANCE);
            producer.send(record);
        }
    }

    // Metodo per inviare messaggi a un topic
    private void sendToTopic(KafkaProducer<String, String> producer, String message, String topic) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), record.headers(), org.example.producer.HeadersTextMapSetter.INSTANCE);
        producer.send(record);
    }

}
