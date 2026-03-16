package org.example;


import java.io.*;
import java.net.MalformedURLException;
import java.util.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingConsumerInterceptor;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.clients.consumer.*;

public class SampleOperator implements Runnable {

    private String topicName;
    private int numPartitions;
    private short replicationFactor;
    private String modifica;
    private int partizioni;
    private Map<String, String> newProperties;
    private int currentP=0;

    public SampleOperator( String t, int n, short r, String m, int p , Map<String, String> nuove){
        topicName = t;
        numPartitions = n;
        replicationFactor = r;
        modifica = m;
        partizioni = p;
        newProperties= nuove;
    }

    public void run(){

        Properties config = new Properties();

        /*
        if(topicName.compareTo("topic2")==0){
            config.put("bootstrap.servers", "192.168.178.119:9092");
        }
        else if(topicName.compareTo("topic3")==0){
            config.put("bootstrap.servers", "192.168.178.119:9093");
        }
        else{
            config.put("bootstrap.servers", "192.168.178.119:9094");
        }

         */




        config.put("bootstrap.servers", "192.168.178.119:9092");
        AdminClient adminClient = AdminClient.create(config);



        if(modifica.compareTo("no")==0){
            //TOPIC CREATA NORMALMENTE

            long startTime = System.nanoTime();
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);

            try {
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                //adminClient.createTopics(Collections.singleton(newTopic));
                long endTime = System.nanoTime();
                long elapsedTime = (endTime - startTime) / 1_000_000; // Convertire da nanosecondi a millisecondi

                System.out.println("Topic " + topicName + " creato con successo!, tempo impiegato: " + elapsedTime + " ms");

                //INVIO I DATI DELLA TOPIC ALL'OPERATORE CENTRALE
                currentP = inviaDatiTopic(adminClient, config, topicName, replicationFactor);


            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
                System.err.println("Si è verificato un errore durante la creazione del topic: " + e.getMessage());
            }


            Properties c = new Properties();
            c.put("bootstrap.servers", "192.168.178.119:9092");
            c.put("group.id", "topicOperator-" + topicName + "-" + System.currentTimeMillis());
            c.put("auto.offset.reset", "earliest");
            c.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            c.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            Consumer<String, String> consumer = new KafkaConsumer<>(c);

            consumer.subscribe(Collections.singletonList("configurazioni"));
            consumer.seekToBeginning(consumer.assignment());

            while(true){
                try {
                    Thread.sleep(50000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    String[] p = record.value().split(";");
                    if(p[0].compareTo("-"+topicName)==0){
                        if(Integer.parseInt(p[1]) > currentP){
                            //MODIFICA LE PARTIZIONI
                            modificaPartizioni(Integer.parseInt(p[1]), adminClient);
                            break;
                        }
                    }
                }

                currentP = inviaDatiTopic(adminClient, config, topicName, replicationFactor);
            }


        }
        else if(modifica.compareTo("configurazioni")==0){
            //MODIFICA DELLE CONFIGURAZIONI

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

            Map<String, AlterConfigOp> alterConfigOps = new HashMap<>();
            newProperties.forEach((key, value) -> {
                alterConfigOps.put(key, new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET));
            });

            try{

            // Modifica delle configurazioni del topic
            AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(Collections.singletonMap(resource, alterConfigOps.values()));
            alterConfigsResult.all().get();

            // Verifica delle nuove configurazioni
            DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singleton(resource));
            Config retrievedConfigs = describeConfigsResult.all().get().get(resource);

            // Stampa delle nuove configurazioni
            System.out.println("Nuove configurazioni per il topic " + topicName + ":");
            retrievedConfigs.entries().forEach(configEntry -> {
                System.out.println(configEntry.name() + " = " + configEntry.value());
            });

            }
            catch (ExecutionException | InterruptedException e){
                e.printStackTrace();
                System.err.println("Si è verificato un errore durante la modifica della topic: " + e.getMessage());
            }

        }
        else if(modifica.compareTo("clienti")==0) {
            //OTTIENE INFO DAI CLIENTI DELL'OPERATOR

            while(true) {

                try {
                    // Ottenere la lista di tutti i consumer groups
                    Collection<ConsumerGroupListing> consumerGroups = adminClient.listConsumerGroups().all().get();

                    for (ConsumerGroupListing groupListing : consumerGroups) {
                        String groupId = groupListing.groupId();
                        System.out.println("Consumer Group ID: " + groupId);

                        // Descrivere ciascun consumer group
                        DescribeConsumerGroupsResult describeConsumerGroupsResult = adminClient.describeConsumerGroups(Collections.singleton(groupId));
                        Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap = describeConsumerGroupsResult.all().get();

                        ConsumerGroupDescription consumerGroupDescription = consumerGroupDescriptionMap.get(groupId);
                        System.out.println("Group ID: " + consumerGroupDescription.groupId());
                        System.out.println("Is Simple Consumer Group: " + consumerGroupDescription.isSimpleConsumerGroup());
                        System.out.println("State: " + consumerGroupDescription.state());
                        System.out.println("Members:");

                        for (MemberDescription memberDescription : consumerGroupDescription.members()) {
                            System.out.println(" - Member ID: " + memberDescription.consumerId());
                            System.out.println("   Client ID: " + memberDescription.clientId());
                            System.out.println("   Host: " + memberDescription.host());
                            System.out.println("   Partitions: " + memberDescription.assignment().topicPartitions());
                        }
                        System.out.println("----------");
                    }

                    TopicPartition topicPartition = new TopicPartition(topicName, 0);

                    // Descrivere i producer per la partizione specificata
                    DescribeProducersResult describeProducersResult = adminClient.describeProducers(Collections.singleton(topicPartition));
                    Map<TopicPartition, DescribeProducersResult.PartitionProducerState> producerStateMap = describeProducersResult.all().get();

                    DescribeProducersResult.PartitionProducerState partitionProducerState = producerStateMap.get(topicPartition);

                    System.out.println("Producer information for topic " + topicName + ", partition " + 0 + ":");
                    for (ProducerState producerState : partitionProducerState.activeProducers()) {
                        System.out.println("Producer ID: " + producerState.producerId());
                        System.out.println("Producer Epoch: " + producerState.producerEpoch());
                        System.out.println("Last Sequence: " + producerState.lastSequence());
                        System.out.println("Last Timestamp: " + producerState.lastTimestamp());
                        System.out.println("Coordinator Epoch: " + producerState.coordinatorEpoch());
                        System.out.println("Current Transaction Start Offset: " + producerState.currentTransactionStartOffset());
                        System.out.println("------------");
                    }

                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    System.err.println("Si è verificato un errore durante la visualizzazione dei clienti: " + e.getMessage());
                }

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

            }


        }
        else if(modifica.compareTo("entry")==0){
            //OTTIENE IL NUMERO DELLE ENTRY IN UNA TOPIC

            while(true){

                try{

                    List<TopicPartition> topicPartitions = getTopicPartitions(adminClient, topicName);

                    if (topicPartitions.isEmpty()) {
                        System.out.println("No partitions found for topic: " + topicName);
                        return;
                    }

                    // Crea richieste per ottenere gli offset dell'inizio e della fine
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> beginningOffsets = adminClient
                            .listOffsets(createOffsetSpecMap(topicPartitions, OffsetSpec.earliest()))
                            .all().get();

                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = adminClient
                            .listOffsets(createOffsetSpecMap(topicPartitions, OffsetSpec.latest()))
                            .all().get();

                    // Calcola il numero di entry per ogni partizione
                    long totalMessages = 0;
                    for (TopicPartition tp : topicPartitions) {
                        long startOffset = beginningOffsets.get(tp).offset();
                        long endOffset = endOffsets.get(tp).offset();
                        totalMessages += (endOffset - startOffset);
                    }

                    System.out.println("Total messages in topic " + topicName + ": " + totalMessages);


                } catch(InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    System.err.println("Si è verificato un errore durante la visualizzazione delle entry: " + e.getMessage());
                }

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

            }

        }
        else if(modifica.compareTo("metriche")==0){
            //MESSAGGI LETTI IN MINUTO SU UNA TOPIC

            config.put("replication.factor", "3");
            config.put("partitions", "3");
            config.put("group.id", "operator" + System.currentTimeMillis());
            config.put("auto.offset.reset", "earliest");
            config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            config.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingConsumerInterceptor.class.getName());

            Consumer<String, String> consumer = new KafkaConsumer<>(config);

            consumer.subscribe(Collections.singletonList(topicName));

            AtomicLong messageCount = new AtomicLong(0);

            // Timer per stampare il numero di messaggi letti ogni minuto
            Timer timer = new Timer(true);
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    long count = messageCount.getAndSet(0);
                    System.out.println("Messaggi letti nell'ultimo minuto: " + count);
                }
            }, 0, 60000);  // Ogni 60000 ms (1 minuto)

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed message: key = %s, value = %s%n", record.key(), record.value());
                    messageCount.incrementAndGet();
                }
            }

        }
        else{
            System.out.println("Parametro Modifica non corretto");
        }

    }

    private static List<TopicPartition> getTopicPartitions(AdminClient adminClient, String topicName) throws ExecutionException, InterruptedException {
        TopicDescription topicDescription = adminClient.describeTopics(Collections.singletonList(topicName))
                .all().get().get(topicName);

        List<TopicPartition> partitions = new ArrayList<>();
        topicDescription.partitions().forEach(partitionInfo -> {
            partitions.add(new TopicPartition(topicName, partitionInfo.partition()));
        });

        return partitions;
    }

    private static Map<TopicPartition, OffsetSpec> createOffsetSpecMap(List<TopicPartition> partitions, OffsetSpec offsetSpec) {
        Map<TopicPartition, OffsetSpec> timestampsToSearch = new HashMap<>();
        for (TopicPartition partition : partitions) {
            timestampsToSearch.put(partition, offsetSpec);
        }
        return timestampsToSearch;
    }

    private static int inviaDatiTopic(AdminClient adminClient, Properties config, String topicName, int replicationFactor){
        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        int currentPartitionCount =0;
        try {
            Collection<Node> nodes = describeClusterResult.nodes().get();
            int brokerCount = nodes.size();


            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(topicName));
            TopicDescription topicDescription = describeTopicsResult.values().get(topicName).get();
            currentPartitionCount = topicDescription.partitions().size();

            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

            // Ottieni le configurazioni della topic
            DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singleton(configResource));
            Map<ConfigResource, Config> configMap = describeConfigsResult.all().get();


            //ottengo info sui clienti
            int k=0;
            Collection<ConsumerGroupListing> consumerGroups = adminClient.listConsumerGroups().all().get();
            for (ConsumerGroupListing groupListing : consumerGroups) {
                String groupId = groupListing.groupId();

                // Descrivere ciascun consumer group
                DescribeConsumerGroupsResult describeConsumerGroupsResult = adminClient.describeConsumerGroups(Collections.singleton(groupId));
                Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap = describeConsumerGroupsResult.all().get();

                ConsumerGroupDescription consumerGroupDescription = consumerGroupDescriptionMap.get(groupId);
                for (MemberDescription memberDescription : consumerGroupDescription.members()) {
                    for (TopicPartition topicPartition : memberDescription.assignment().topicPartitions()) {
                        // Controlla se il consumatore sta consumando dalla topic di interesse
                        if (topicPartition.topic().equals(topicName)) {
                            k++;
                            break;
                        }
                    }
                }
            }


            config.put("bootstrap.servers", "192.168.178.119:9092");
            config.put("replication.factor", "3");
            config.put("partitions", "3");
            config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> producer = new KafkaProducer<>(config);

            //topic;nBroker;partizioni;nClienti;repliche;configurazioni
            ProducerRecord<String, String> record = new ProducerRecord<>("configurazioni", topicName + ";" + brokerCount + ";" + currentPartitionCount + ";" + k + ";" + replicationFactor);
            producer.send(record);

            producer.close();

        }catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }

        return currentPartitionCount;
    }

    private void modificaPartizioni(int num, AdminClient adminClient){

        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(topicName));
        try{
            TopicDescription topicDescription = describeTopicsResult.values().get(topicName).get();
            int currentPartitionCount = topicDescription.partitions().size();

            if (num > currentPartitionCount) {
                long startTime = System.nanoTime();
                NewPartitions newPartitions = NewPartitions.increaseTo(num);
                adminClient.createPartitions(Collections.singletonMap(topicName, newPartitions)).all().get();
                long endTime = System.nanoTime();
                long elapsedTime = (endTime - startTime) / 1_000_000; // Convertire da nanosecondi a millisecondi
                System.out.println("Numero delle partizioni della topic aumentato a " + (num) + ", tempo impiegato: " + elapsedTime + " ms");
            }

        }
        catch (ExecutionException | InterruptedException e){
            e.printStackTrace();
            System.err.println("Si è verificato un errore durante la modifica della topic: " + e.getMessage());
        }
    }

}
