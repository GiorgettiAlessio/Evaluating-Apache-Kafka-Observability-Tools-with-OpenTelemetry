package org.example;


import java.time.Duration;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.ResourceAttributes;
import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import java.util.*;

import static com.google.common.primitives.Doubles.max;

public class Runner {
    public static void main(String[] args) throws InterruptedException {



        Resource resource = Resource.getDefault().merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "my-kafka-service")));

        Duration schedule = Duration.ofMillis(1000);
        Duration timeout = Duration.ofMillis(10000);

        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(JaegerGrpcSpanExporter.builder().build())
                        .setMaxQueueSize(10000)
                        .setScheduleDelay(schedule)
                        .setMaxExportBatchSize(1000)
                        .setExporterTimeout(timeout)
                        .build())
                .setSampler(Sampler.alwaysOn())
                .setResource(resource)
                .build();



        SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder()
                .registerMetricReader(
                        PeriodicMetricReader
                                .builder(LoggingMetricExporter.create())
                                .setInterval(Duration.ofSeconds(10)).build())
                .build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .setMeterProvider(sdkMeterProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal();





        List<String> daScrivere = new ArrayList<String>();
        List<String> daScrivere1 = new ArrayList<String>();


        int numMessaggi=1400;

        for(int i=0;i<numMessaggi;i++){
            daScrivere.add("Messaggio" + i);
        }

        for(int i=0;i<numMessaggi;i++){
            daScrivere1.add("m" + i + "-3");
        }

        Map<String, String> topicConfigurations = new HashMap<>();
        // Aggiunta di configurazioni
        topicConfigurations.put("cleanup.policy", "compact");
        topicConfigurations.put("retention.ms", "86400000"); // 1 giorno in millisecondi
        topicConfigurations.put("max.message.bytes", "1048576"); // 1 MB
        topicConfigurations.put("compression.type", "gzip");

        SampleOperator instanzaO = new SampleOperator("topic1", 5, (short) 1, "no", 0 , topicConfigurations);
        Thread threadO = new Thread(instanzaO);
        threadO.start();


        SampleOperator instanzaO_1 = new SampleOperator("topic2", 5, (short) 1, "no", 0 , topicConfigurations);
        Thread threadO_1 = new Thread(instanzaO_1);
        threadO_1.start();


        SampleOperator instanzaO_2 = new SampleOperator("topic3", 5, (short) 1, "no", 0 , topicConfigurations);
        Thread threadO_2 = new Thread(instanzaO_2);
        threadO_2.start();

        SampleOperator instanzaO_3 = new SampleOperator("topic4", 5, (short) 1, "no", 0 , topicConfigurations);
        Thread threadO_3 = new Thread(instanzaO_3);
        threadO_3.start();


        SampleOperator instanzaO_4 = new SampleOperator("topic5", 5, (short) 1, "no", 0 , topicConfigurations);
        Thread threadO_4 = new Thread(instanzaO_4);
        threadO_4.start();



        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("sveglia");


        SampleConsumer C1 = new SampleConsumer("C1", "topic1", numMessaggi);
        Thread threadC1 = new Thread(C1);
        threadC1.start();


        SampleProducer P1 = new SampleProducer("P1", daScrivere, "topic1");
        Thread threadP1 = new Thread(P1);
        threadP1.start();


        SampleProducer P2 = new SampleProducer("P2", daScrivere1, "topic1");
        Thread threadP2 = new Thread(P2);
        threadP2.start();


        SampleConsumer C2 = new SampleConsumer("C2", "topic2", numMessaggi);
        SampleConsumer C3 = new SampleConsumer("C3", "topic3", numMessaggi);
        SampleConsumer C4 = new SampleConsumer("C4", "topic4", numMessaggi);
        SampleConsumer C5 = new SampleConsumer("C5", "topic5", numMessaggi);



        Thread threadC2 = new Thread(C2);
        Thread threadC3 = new Thread(C3);
        Thread threadC4 = new Thread(C4);
        Thread threadC5 = new Thread(C5);

        threadC2.start();
        threadC3.start();
        threadC4.start();
        threadC5.start();




        try {
            Thread.sleep(40000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }


        CentralOperator instanzaCentral = new CentralOperator(numMessaggi);
        Thread threadCentral = new Thread(instanzaCentral);
        threadCentral.start();




    }
}
