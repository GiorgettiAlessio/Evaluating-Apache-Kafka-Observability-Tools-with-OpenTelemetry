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


public class producer implements Runnable {


    private static final Tracer tracer = GlobalOpenTelemetry.getTracer("TracingProducer");

    public producer(){

    }

    public void run(){

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.178.119:9092");
        properties.put("replication.factor", "3");
        properties.put("partitions", "3");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingProducerInterceptor.class.getName());


        Producer<String, String> producer = new KafkaProducer<>(properties);



        for (int i = 0; i < 100; i++) {
            Span span = tracer.spanBuilder("send-message").startSpan();
            try (Scope scope = span.makeCurrent()) {
                // Inserire il contesto della span nel messaggio
                ProducerRecord<String, String> record = new ProducerRecord<>("t", "key" + i, "value" + i);
                GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), record.headers(), HeadersTextMapSetter.INSTANCE);
                producer.send(record);
            } finally {
                span.end();
            }
        }

        // Close the producer
        producer.close();


    }

    static class HeadersTextMapSetter implements TextMapSetter<org.apache.kafka.common.header.Headers> {
        static final HeadersTextMapSetter INSTANCE = new HeadersTextMapSetter();

        @Override
        public void set(org.apache.kafka.common.header.Headers headers, String key, String value) {
            headers.add(key, value.getBytes());
        }
    }

}
