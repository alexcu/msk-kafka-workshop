package org.msk.workshop.producer.sr.json;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.DataFormat;
import org.apache.avro.Schema.Parser;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class Producer {
    private final static Logger logger = LoggerFactory.getLogger(Producer.class.getName());
    private KafkaProducer<String, String> producer;
    private Properties applicationProperties;
    private Map<String,String> propertiesMap;
    private final AtomicBoolean shouldStop = new AtomicBoolean(false);
    private String bootstrapServers;
    public Producer(String bootstrapServers){
        this.bootstrapServers = bootstrapServers;
        this.propertiesMap = new HashMap<String,String>();
        propertiesMap.put("mw-1","Listing-mountwaverley");
        propertiesMap.put("gw-2","Listing-Glenwaverley");
        propertiesMap.put("bl-3","Listing-Boxhill");
        propertiesMap.put("bb-4","Listing-Blackburn");
        propertiesMap.put("cr-5","Listing-carlton");
        propertiesMap.put("st-6","List-st.Kilda");
    }
    private void readProperties() throws IOException {
        logger.info("reading app.properties...");
        InputStream iStream = this.getClass().getClassLoader().getResourceAsStream("app.properties");
        this.applicationProperties = new Properties();
        this.applicationProperties.load(iStream);
    }
    public Properties buildProducerConfig() throws IOException{
        logger.info("building producer configuration...");
        if(this.applicationProperties == null)
            this.readProperties();
        Properties producerProperties = new Properties();
        //for async client, use the following "software.amazon.awssdk.http.async.service.impl" and "software.amazon.awssdk.http.nio.netty.NettySdkAsyncHttpService"
        System.setProperty("software.amazon.awssdk.http.service.impl", "software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService");
        producerProperties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, this.applicationProperties.getProperty("CLIENT_ID"));
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG,this.applicationProperties.getProperty("NUMBER_OF_ACKS"));

        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
        producerProperties.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.JSON.name());
        producerProperties.put(AWSSchemaRegistryConstants.AWS_REGION, this.applicationProperties.getProperty("AWS_REGION"));
        producerProperties.put(AWSSchemaRegistryConstants.REGISTRY_NAME, this.applicationProperties.getProperty("SCHEMA_REGISTRY_NAME"));
        producerProperties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, this.applicationProperties.getProperty("SCHEMA_NAME"));
        producerProperties.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        producerProperties.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, Compatibility.FULL);
        return producerProperties;
    }
    public void startProducers() throws IOException, InterruptedException {
        Properties producerConfig = this.buildProducerConfig();
        int numberOfMessages = Integer.valueOf(this.applicationProperties.getProperty("NUMBER_OF_MESSAGES")).intValue();
        String topic = this.applicationProperties.getProperty("TOPIC");
        logger.info("reading customer avro schema...");
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("Listings.avsc");
        Schema schema_listing = new Parser().parse(inputStream);
        int counter = 0;

        String jsonSchema = "{\n" + "        \"$schema\": \"http://json-schema.org/draft-04/schema#\",\n"
                + "        \"type\": \"object\",\n" + "        \"properties\": {\n" + "          \"employee\": {\n"
                + "            \"type\": \"object\",\n" + "            \"properties\": {\n"
                + "              \"name\": {\n" + "                \"type\": \"string\"\n" + "              },\n"
                + "              \"age\": {\n" + "                \"type\": \"integer\"\n" + "              },\n"
                + "              \"city\": {\n" + "                \"type\": \"string\"\n" + "              }\n"
                + "            },\n" + "            \"required\": [\n" + "              \"name\",\n"
                + "              \"age\",\n" + "              \"city\"\n" + "            ]\n" + "          }\n"
                + "        },\n" + "        \"required\": [\n" + "          \"employee\"\n" + "        ]\n"
                + "      }";
        String jsonPayload = "{\n" + "        \"employee\": {\n" + "          \"name\": \"John\",\n" + "          \"age\": 30,\n"
                + "          \"city\": \"New York\"\n" + "        }\n" + "      }";

        //JsonDataWithSchema jsonSchemaWithData = JsonDataWithSchema.builder(jsonSchema, jsonPayload);
        JsonDataWithSchema jsonSchemaWithData = JsonDataWithSchema.builder(jsonSchema, jsonPayload).build();

        List<JsonDataWithSchema> genericJsonRecords = new ArrayList<>();
        genericJsonRecords.add(jsonSchemaWithData);

        try (KafkaProducer<String, JsonDataWithSchema> producer = new KafkaProducer<String, JsonDataWithSchema>(producerConfig)) {
            for (int i = 0; i < genericJsonRecords.size(); i++) {
                JsonDataWithSchema r = genericJsonRecords.get(i);

                final ProducerRecord<String, JsonDataWithSchema> record;
                record = new ProducerRecord<String, JsonDataWithSchema>(topic, "message-" + i, r);

                producer.send(record);
                System.out.println("Sent message " + i);
                Thread.sleep(1000L);
            }
            producer.flush();
            System.out.println("Successfully produced 10 messages to a topic called " + topic);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean checkPrime(int number) {
        return number > 1
                && IntStream.rangeClosed(2, (int) Math.sqrt(number))
                .noneMatch(n -> (number % n == 0));
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        //it expects args[0] as a broker address
        Producer producer = new Producer(args[0]);
        producer.startProducers();
    }
}