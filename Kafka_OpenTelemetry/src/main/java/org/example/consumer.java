package org.example;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingConsumerInterceptor;
import org.apache.kafka.clients.consumer.*;
import io.opentelemetry.api.trace.Span;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.*;

import java.time.Duration;
import java.util.Properties;

public class consumer implements Runnable {


    private static final Tracer tracer = GlobalOpenTelemetry.getTracer("TracingConsumer");




    public consumer(){

    }


    public void run() {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.178.119:9092");
        properties.put("replication.factor", "3");
        properties.put("partitions", "3");
        properties.put("group.id", "prova_group" + System.currentTimeMillis());
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingConsumerInterceptor.class.getName());

        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList("t"));
        consumer.seekToBeginning(consumer.assignment());

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                Context context = GlobalOpenTelemetry.getPropagators().getTextMapPropagator().extract(Context.current(), record.headers(), HeadersTextMapGetter.INSTANCE);
                Span span = tracer.spanBuilder("process-message").setParent(context).startSpan();
                try (Scope scope = span.makeCurrent()) {
                    // Elaborazione del messaggio
                    System.out.printf("key = %s, value = %s, offset = %d%n", record.key(), record.value(), record.offset());
                } finally {
                    span.end();
                }
            }
        }



    }

    static class HeadersTextMapGetter implements TextMapGetter<org.apache.kafka.common.header.Headers> {
        static final HeadersTextMapGetter INSTANCE = new HeadersTextMapGetter();

        @Override
        public Iterable<String> keys(org.apache.kafka.common.header.Headers headers) {
            return () -> new Iterator<String>() {
                private final Iterator<Header> iterator = headers.iterator();

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public String next() {
                    return iterator.next().key();
                }
            };
        }

        @Override
        public String get(org.apache.kafka.common.header.Headers headers, String key) {
            Header header = headers.lastHeader(key);
            return header != null ? new String(header.value()) : null;
        }
    }
}
