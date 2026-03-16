package org.example;


import com.fasterxml.jackson.databind.JsonNode;
import io.opentelemetry.api.OpenTelemetry;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.io.*;
import java.lang.reflect.Array;
import java.net.http.*;
import java.util.*;

import static com.google.common.primitives.Doubles.max;


public class CentralOperator implements Runnable {

    int numMess=0;

    public CentralOperator(int num){
        numMess=num;
    }

    public void run(){

        Properties config = new Properties();
        config.put("bootstrap.servers", "192.168.178.119:9092");

        config.put("replication.factor", "3");
        config.put("partitions", "3");
        config.put("group.id", "operator" + System.currentTimeMillis());
        config.put("auto.offset.reset", "earliest");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<String, String> consumer = new KafkaConsumer<>(config);

        consumer.subscribe(Collections.singletonList("configurazioni"));
        consumer.seekToBeginning(consumer.assignment());

        int nB=0, Bmax=5;;
        int b=0;

        while(true) {

            int h=0;
            int agg=0;


            //LEGGO LE CONFIGURAZIONI DELLE TOPIC INVIATE DAGLI OPERATORI
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("letto: " + record.value());

                //topic;nBroker;partizioni;nClienti;repliche;configurazioni
                String[] p = record.value().split(";");
                if(!p[0].contains("-")){
                    agg++;
                    nB = Integer.parseInt(p[1]);
                    h += ottimizza(p[0], Integer.parseInt(p[3]), Integer.parseInt(p[4]) ,Bmax);
                }
            }

            System.out.println("broker attuali: " + nB + " broker richiesti: " + h);


            //AGGIUNGO I BROKER MANCANTI
            if(h > nB && h <= Bmax && agg == 5){
                b=h;
                System.out.println("aggiungo i " + (h-nB) + " broker richiesti");
                while(h-nB > 0){
                    aggiungoBroker(h-2);
                    h--;
                }
            }

            nB=b;

            //SCARICO I DATI DA JAEGER
            // Definisci l'endpoint di Jaeger
            String jaegerEndpoint = "http://localhost:16686";

            // Esegui la query per recuperare le trace e le span
            String queryUrl = jaegerEndpoint + "/api/traces?service=my-kafka-service&limit=2800"; // Esempio di query per recuperare le trace di un servizio


            CloseableHttpClient httpClient = HttpClientBuilder.create().build();
            HttpGet httpGet = new HttpGet(queryUrl);

            // Esegui la richiesta HTTP
            String jsonResponse = null;

            Map<String, Integer> messaggiOrdinati = new LinkedHashMap<>();



            Map<String, JsonNode> processSpansT2 = new HashMap<>();
            List<String> traceDifettoseT2 = new ArrayList<>();


            Map<String, JsonNode> publishSpansT5 = new HashMap<>();
            Map<String, JsonNode> processSpansT5 = new HashMap<>();
            List<String> traceDifettoseT5 = new ArrayList<>();


            //ANALIZZO IL JSON
            try {
                // Esecuzione della richiesta e lettura della risposta
                jsonResponse = EntityUtils.toString(httpClient.execute(httpGet).getEntity());


                try (PrintWriter writer = new PrintWriter(new FileWriter("/home/alessio/Documents/log"))) {
                    writer.println(jsonResponse);
                }catch (IOException e) {
                    e.printStackTrace();
                }



                // Crea un ObjectMapper
                ObjectMapper objectMapper = new ObjectMapper();
                // Leggi la stringa JSON in un JsonNode
                JsonNode rootNode = objectMapper.readTree(jsonResponse);
                // Accedi al nodo "data"
                JsonNode dataNode = rootNode.path("data");
                // Itera attraverso gli elementi in "data"



                for (JsonNode dataElement : dataNode) {

                    int j=0;
                    int k=0;
                    boolean t1t5=false;

                    String traceID = dataElement.get("traceID").asText();

                    JsonNode spansNode = dataElement.path("spans");
                    // Itera attraverso gli elementi in "spans"
                    for (JsonNode span : spansNode) {
                        String operationName = span.get("operationName").asText();

                        //LOGICA PER MESSAGGI DIFETTOSI E PER METTERLI IN ORDINE
                        if(operationName.contains("-3")){
                            t1t5=true;
                            String[] p = operationName.split("-");
                            String[] p1 = p[2].split("m");
                            messaggiOrdinati.put(traceID, Integer.parseInt(p1[1]));
                        }
                        else if(operationName.contains("Messaggio")){
                            String[] p = operationName.split("Messaggio");
                            messaggiOrdinati.put(traceID, Integer.parseInt(p[1]));
                        }

                        if (operationName.contains("topic1 publish")) {
                            publishSpansT5.put(traceID, span);
                            j++;
                            k++;
                        } else if (operationName.contains("topic5 process")) {
                            processSpansT5.put(traceID, span);
                            j++;
                        }else if(operationName.contains("topic2 process")){
                            processSpansT2.put(traceID, span);
                            k++;
                        }

                    }

                    if(j==1 && t1t5){
                        traceDifettoseT5.add(traceID);
                    }
                    else if(k==1 && !t1t5){
                        traceDifettoseT2.add(traceID);
                    }



                }


            } catch (IOException e) {
                e.printStackTrace();
            }


            int i=0;
            int u=0;
            long sommaT2=0;
            long mediaT2=0;
            long sommaT5=0;
            long mediaT5=0;
            Map<String, Long> risultatiT2 = new HashMap<>();
            Map<String, Long> risultatiT5 = new HashMap<>();
            List<Long> risT2 = new ArrayList<>(numMess);
            List<Long> risT5 = new ArrayList<>(numMess);

            for (int q = 0; q < numMess; q++) {
                risT2.add(null);
                risT5.add(null);
            }


            int s=0;
            List<Long> startTimes = new ArrayList<>();

            // Calcolo della differenza di tempo per ogni traceID
            for (String traceID : publishSpansT5.keySet()) {
                if (processSpansT5.containsKey(traceID)) {
                    long publishStartTime = publishSpansT5.get(traceID).get("startTime").asLong();
                    long processStartTime = processSpansT5.get(traceID).get("startTime").asLong();

                    startTimes.add(publishStartTime);
                    startTimes.add(processStartTime);

                    long timeDifference = processStartTime - publishStartTime;
                    sommaT5 += timeDifference;
                    i++;
                    //System.out.println("(T1-T5) Trace ID: " + traceID + " | Time Difference: " + timeDifference + " μs");
                    risultatiT5.put("T1-T5," + s, timeDifference);
                    s++;

                    //RIEMPIO L'ARRAY DELLA DURATA DEI MESSAGGI IN ORDINE
                    risT5.set(messaggiOrdinati.get(traceID), timeDifference);

                }
                else if(processSpansT2.containsKey(traceID)){
                    long publishStartTime = publishSpansT5.get(traceID).get("startTime").asLong();
                    long processStartTime = processSpansT2.get(traceID).get("startTime").asLong();

                    startTimes.add(publishStartTime);
                    startTimes.add(processStartTime);

                    long timeDifference = processStartTime - publishStartTime;
                    sommaT2 += timeDifference;
                    u++;
                    //System.out.println("(T1-T2) Trace ID: " + traceID + " | Time Difference: " + timeDifference + " μs");
                    risultatiT2.put("T1-T2," + s, timeDifference);
                    s++;


                    //RIEMPIO L'ARRAY DELLA DURATA DEI MESSAGGI IN ORDINE
                    risT2.set(messaggiOrdinati.get(traceID), timeDifference);
                }
            }

            System.out.println("processate correttamente " + u + " span T1-T2");
            System.out.println("processate correttamente " + i + " span T1-T5");
            System.out.println("------------------------------------------------------------------------");

            mediaT5 = sommaT5/i;
            mediaT2 = sommaT2/u;



            for(String traccia : traceDifettoseT5){
                //System.out.println("(T1-T5) Trace ID: " + traccia + " | Time Difference: " + mediaT5 + " μs");
                risultatiT5.put("T1-T5," + s, mediaT5);
                s++;

                risT5.set(messaggiOrdinati.get(traccia), mediaT5);
            }

            for(String traccia : traceDifettoseT2){
                //System.out.println("(T1-T2) Trace ID: " + traccia + " | Time Difference: " + mediaT2 + " μs");
                risultatiT2.put("T1-T2," + s, mediaT2);
                s++;

                risT2.set(messaggiOrdinati.get(traccia), mediaT2);
            }

            // Trova il tempo minimo e massimo
            long minStartTime = Collections.min(startTimes);
            long maxStartTime = Collections.max(startTimes);

            // Calcola la finestra temporale in secondi
            long timeWindowMicros = maxStartTime - minStartTime;
            double timeWindowSeconds = timeWindowMicros / 1_000_000.0;

            double throughput = (numMess*2) / timeWindowSeconds;
            System.out.println("Throughput: " + throughput + " messaggi al secondo");

            System.out.println("------------------------------------------------------------------------");



            //media tempo di coda T1-T2
            Map<String, Long> sumMap = new HashMap<>();
            Map<String, Integer> countMap = new HashMap<>();

            // Popolamento delle mappe
            for (Map.Entry<String, Long> entry : risultatiT2.entrySet()) {
                String[] p = entry.getKey().split(",");
                String key = p[0];
                Long interval = entry.getValue();

                sumMap.put(key, sumMap.getOrDefault(key, 0L) + interval);
                countMap.put(key, countMap.getOrDefault(key, 0) + 1);
            }

            // Calcolo delle medie
            Map<String, Double> averageMapT2 = new HashMap<>();
            for (String key : sumMap.keySet()) {
                double average = (double) sumMap.get(key) / countMap.get(key);
                averageMapT2.put(key, average);
            }

            //media tempo di coda T1-T5
            sumMap.clear();
            countMap.clear();

            // Popolamento delle mappe
            for (Map.Entry<String, Long> entry : risultatiT5.entrySet()) {
                String[] p = entry.getKey().split(",");
                String key = p[0];
                Long interval = entry.getValue();

                sumMap.put(key, sumMap.getOrDefault(key, 0L) + interval);
                countMap.put(key, countMap.getOrDefault(key, 0) + 1);
            }

            // Calcolo delle medie
            Map<String, Double> averageMapT5 = new HashMap<>();
            for (String key : sumMap.keySet()) {
                double average = (double) sumMap.get(key) / countMap.get(key);
                averageMapT5.put(key, average);
            }



            Map<String, String> mediaViaggi = new HashMap<>();

            // Stampa delle medie
            for (Map.Entry<String, Double> entry : averageMapT2.entrySet()) {
                System.out.println("Topic: " + entry.getKey() + ", Media Intervallo: " + entry.getValue() + " μs");
                mediaViaggi.put(entry.getKey(), entry.getValue().toString());
            }
            for (Map.Entry<String, Double> entry : averageMapT5.entrySet()) {
                System.out.println("Topic: " + entry.getKey() + ", Media Intervallo: " + entry.getValue() + " μs");
                mediaViaggi.put(entry.getKey(), entry.getValue().toString());
            }




            //CREO FILE CSV
            String[] header = { "Topic", "Latenza Media" };
            // Creare e scrivere nel file CSV
            try (PrintWriter writer = new PrintWriter(new FileWriter("/home/alessio/Documents/latenzaMedia.csv"))) {
                // Scrivere l'header
                writer.println(String.join(",", header));

                // Scrivere i dati
                for (Map.Entry<String, String> row : mediaViaggi.entrySet()) {
                    writer.println(row.getKey()+ "," + row.getValue());
                }

                System.out.println("File CSV creato con successo.");
            } catch (IOException e) {
                e.printStackTrace();
            }


            String[] header1 = { "Topic", "Latenza" };
            // Creare e scrivere nel file CSV
            try (PrintWriter writer = new PrintWriter(new FileWriter("/home/alessio/Documents/latenza_tutti.csv"))) {
                // Scrivere l'header
                writer.println(String.join(",", header1));
                // Scrivere i dati
                for (Map.Entry<String, Long> row : risultatiT2.entrySet()) {
                    String[] p = row.getKey().split(",");
                    writer.println(p[0] + "," + row.getValue().toString());
                }
                for (Map.Entry<String, Long> row : risultatiT5.entrySet()) {
                    String[] p = row.getKey().split(",");
                    writer.println(p[0] + "," + row.getValue().toString());
                }

                System.out.println("File CSV creato con successo.");
            } catch (IOException e) {
                e.printStackTrace();
            }

            String[] header2 = { "Topic", "Latenza Ordinata" };
            // Creare e scrivere nel file CSV
            try (PrintWriter writer = new PrintWriter(new FileWriter("/home/alessio/Documents/latenza_ordinata.csv"))) {
                // Scrivere l'header
                writer.println(String.join(",", header));

                // Scrivere i dati
                int k=0;
                for (k=0; k<risT2.size();k++){
                    writer.println(k+ "," + risT2.get(k));
                }
                for (k = 0 ; k < risT5.size(); k++){
                    writer.println(k+ "," + risT5.get(k));
                }


                System.out.println("File CSV creato con successo.");
            } catch (IOException e) {
                e.printStackTrace();
            }


            mostraGrafici();




            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }



        }
    }

    private static void mostraGrafici() {
        try {
            // Comando da eseguire
            String command = "nohup /bin/python3 /home/alessio/IdeaProjects/Prova/intervallo-topics.py > kafka_output.log 2>&1 &";

            // Costruzione del ProcessBuilder
            ProcessBuilder processBuilder = new ProcessBuilder("bash", "-c", command);
            processBuilder.redirectErrorStream(true);

            // Avvio del processo
            Process process = processBuilder.start();

            // Lettura dell'output del comando dal file di log
            Thread.sleep(2000); // Attendere un po' per consentire al comando di generare output
            try (BufferedReader logReader = new BufferedReader(new InputStreamReader(
                    new FileInputStream("kafka_output.log")))) {
                String line;
                while ((line = logReader.readLine()) != null) {
                    System.out.println(line);
                }
            }

            // Attesa della terminazione del processo
            int exitCode = process.waitFor();
            System.out.println("Exit code: " + exitCode);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int ottimizza(String topic, int clienti, int repliche, int maxBroker){

        int  c=clienti, r=repliche, U=2, B=maxBroker;
        double L=0.2, H=10000, lr=0.001, u=0.005, T=100000000, Tp=10000000, Tc=20000000;
        int P=0, b=0;

        outerLoop:
        for (b=r; b<=B; b++){
            for(P= (int)((b*H)/r); P >= max((T/Tp), (T/Tc), c); P--){
                if(P*r*lr <= b*L && P*u <= b*U){
                    break outerLoop;
                }
            }
        }


        Properties con = new Properties();
        con.put("bootstrap.servers", "192.168.178.119:9092");
        con.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        con.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(con);

        ProducerRecord<String, String> record = new ProducerRecord<>("configurazioni", "-" + topic + ";" + P);

        producer.send(record);
        producer.close();

        return b;
    }

    private void aggiungoBroker(int n){
        //CREAZIONE DI UN NUOVO BROKER SULLA MACCHINA

        try {
            // Comando da eseguire
            String command = "nohup /home/alessio/Kafka/prova"+n+"/bin/kafka-server-start.sh /home/alessio/Kafka/prova"+n+"/config/server.properties > kafka_output.log 2>&1 &";

            // Costruzione del ProcessBuilder
            ProcessBuilder processBuilder = new ProcessBuilder("bash", "-c", command);
            processBuilder.redirectErrorStream(true);

            // Avvio del processo
            Process process = processBuilder.start();

            // Lettura dell'output del comando dal file di log
            Thread.sleep(2000); // Attendere un po' per consentire al comando di generare output
            try (BufferedReader logReader = new BufferedReader(new InputStreamReader(
                    new FileInputStream("kafka_output.log")))) {
                String line;
                while ((line = logReader.readLine()) != null) {
                    System.out.println(line);
                }
            }

            // Attesa della terminazione del processo
            int exitCode = process.waitFor();
            System.out.println("Exit code: " + exitCode);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
