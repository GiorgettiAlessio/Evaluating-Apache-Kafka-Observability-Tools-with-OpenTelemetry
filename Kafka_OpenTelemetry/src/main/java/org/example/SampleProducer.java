package org.example;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingConsumerInterceptor;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingProducerInterceptor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;


import io.opentelemetry.api.OpenTelemetry;
import org.apache.kafka.common.header.Headers;


public class SampleProducer implements Runnable {

    private String nome;
    private List<String> daScrivcere;
    String topic;
    private static final Tracer tracer = GlobalOpenTelemetry.getTracer("TracingProducer");

    public SampleProducer(String n, List<String> d, String t){
        nome=n;
        daScrivcere=d;
        topic=t;
    }

    public void run(){

        Properties properties = new Properties();
        if(nome.compareTo("P1")==0)
            properties.put("bootstrap.servers", "192.168.178.119:9093");
        else if(nome.compareTo("P2")==0)
            properties.put("bootstrap.servers", "192.168.178.119:9094");

        properties.put("replication.factor", "3");
        properties.put("partitions", "3");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingProducerInterceptor.class.getName());


        Producer<String, String> producer = new KafkaProducer<>(properties);

        for(int i=0; i<400; i++){
            String mess="";
            if(nome.compareTo("P1")==0){
                mess = "ciao " + i;
            }
            else if(nome.compareTo("P2")==0){
                mess = "ciao" + i + "-3";
            }
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, mess);
            producer.send(record);
        }


        try {
            System.out.println("mandati i messaggi di riscaldamento");
            Thread.sleep(5000);

            for (int i = 0; i < daScrivcere.size(); i++) {
                String mess = daScrivcere.get(i);


                //PER MESSAGGI INCREMENTALI AL SECONDO
                if(i==50 || i==200 || i==600){
                    System.out.println("mandati i primi " + i + "messaggi");
                    Thread.sleep(2000);
                }

                int messagesPerSecond;
                if (i < 50) {
                    messagesPerSecond = 10;
                } else if (i < 200) {
                    messagesPerSecond = 50;
                } else if (i < 600) {
                    messagesPerSecond = 100;
                } else if (i < 1000) {
                    messagesPerSecond = 200;
                } else {
                    messagesPerSecond = 200;
                }
                long intervalMillis = 1000 / messagesPerSecond;



                // Crea una nuova span per ogni messaggio
                Span span = tracer.spanBuilder("produce-message-" + mess).setAttribute("messaging.destination.name", topic).setAttribute("messaging.operation", "publish").startSpan();

                try (Scope scope = span.makeCurrent()) {
                    // Crea il messaggio da inviare

                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, mess);

                    // Inietta il contesto di tracing nel messaggio
                    GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), record.headers(), org.example.producer.HeadersTextMapSetter.INSTANCE);

                    // Invia il messaggio
                    producer.send(record);
                } finally {
                    // Chiudi la span dopo l'invio del messaggio
                    span.end();
                }

                //attendi prima di inviare il prossimo messaggio
                Thread.sleep(intervalMillis);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }



        // Close the producer
        producer.close();


    }


}
