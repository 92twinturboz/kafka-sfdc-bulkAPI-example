package com.example;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.config.SaslConfigs;


public class ConsumerAndBulkAPI {
    public static void main(final String[] args) throws Exception {

        String topic = "sfdc-account";
        String sfdcUsername = "mysfdcaccount";
        String sfdcPassword = "mysfdcpassword";
        String sobjectType = "Invoice";
        String apiKey = "mykey";
        String apiSecret = "mysecret";
        String saslJaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + apiKey + "\" password=\"" + apiSecret + "\";";
        // Load properties from a local configuration file
        // Create the configuration file (e.g. at '$HOME/.confluent/java.config') with configuration parameters
        // to connect to your Kafka cluster, which can be on your local host, Confluent Cloud, or any other cluster.
        // Follow these detailed instructions to properly create this file: https://github.com/confluentinc/configuration-templates/tree/master/README.md
        Properties props = new Properties();
        // ccloud kafka properties
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-43n10.us-central1.gcp.confluent.cloud:9092");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "500");
        props.put("security.protocol", "SASL_SSL");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        props.put("ssl.endpoint.identification.algorithm", "https");
        // Add additional properties.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, SalesforceAccount.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sfdc-bulk-api-sink-1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Set very high so the consumer will wait until a sufficiently large batch is available
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 2000000);
        // Also set high to allow as many records to be collected as possible; will batch up and consume records once time limit is reached, regardless of size
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 200000);

        final Consumer<String, SalesforceAccount> consumer = new KafkaConsumer<String, SalesforceAccount>(props);
        consumer.subscribe(Arrays.asList(topic));

        try {
            String csvStr = "";
            while (true) {
                ConsumerRecords<String, SalesforceAccount> records = consumer.poll(10000);
                String bulkJson = "[";
                for (ConsumerRecord<String, SalesforceAccount> record : records) {
                    String key = record.key();
                    System.out.printf("Consumed record with key %s and value %s%n", key, record.value());
                    bulkJson += record.value().toString() + ",";
                }
                bulkJson += "]";
                if(bulkJson.length() > 4) {
                    System.out.println("BULK JSON PAYLOAD: " + bulkJson);
                    ConsumerAndBulkAPI example = new ConsumerAndBulkAPI();
                    BulkConnection connection = example.getBulkConnection(sfdcUsername, sfdcPassword);
                    JobInfo job = example.createJob(sobjectType, connection);
                    List<BatchInfo> batchInfoList = example.createBatchesFromKafka(connection, job, bulkJson);
                    example.closeJob(connection, job.getId());
                    example.awaitCompletion(connection, job, batchInfoList);
                    example.checkResults(connection, job, batchInfoList);
                }
                else {
                    System.out.println("JSON Payload not big enough for bulk request.");
                }
            }

        } finally {
            consumer.close();
        }
    }
    /**
     * Gets the results of the operation and checks for errors.
     */
    private void checkResults(BulkConnection connection, JobInfo job,
                              List<BatchInfo> batchInfoList)
            throws AsyncApiException, IOException {
        // batchInfoList was populated when batches were created and submitted
        for (BatchInfo b : batchInfoList) {
            CSVReader rdr =
                    new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()));
            List<String> resultHeader = rdr.nextRecord();
            int resultCols = resultHeader.size();

            List<String> row;
            while ((row = rdr.nextRecord()) != null) {
                Map<String, String> resultInfo = new HashMap<String, String>();
                for (int i = 0; i < resultCols; i++) {
                    resultInfo.put(resultHeader.get(i), row.get(i));
                }
                boolean success = Boolean.valueOf(resultInfo.get("Success"));
                boolean created = Boolean.valueOf(resultInfo.get("Created"));
                String id = resultInfo.get("Id");
                String error = resultInfo.get("Error");
                if (success && created) {
                    System.out.println("Created row with id " + id);
                } else if (!success) {
                    System.out.println("Failed with error: " + error);
                }
            }
        }
    }



    private void closeJob(BulkConnection connection, String jobId)
            throws AsyncApiException {
        JobInfo job = new JobInfo();
        job.setId(jobId);
        job.setState(JobStateEnum.Closed);
        connection.updateJob(job);
    }



    /**
     * Wait for a job to complete by polling the Bulk API.
     *
     * @param connection
     *            BulkConnection used to check results.
     * @param job
     *            The job awaiting completion.
     * @param batchInfoList
     *            List of batches for this job.
     * @throws AsyncApiException
     */
    private void awaitCompletion(BulkConnection connection, JobInfo job,
                                 List<BatchInfo> batchInfoList)
            throws AsyncApiException {
        long sleepTime = 0L;
        Set<String> incomplete = new HashSet<String>();
        for (BatchInfo bi : batchInfoList) {
            incomplete.add(bi.getId());
        }
        while (!incomplete.isEmpty()) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {}
            System.out.println("Awaiting results..." + incomplete.size());
            sleepTime = 10000L;
            BatchInfo[] statusList =
                    connection.getBatchInfoList(job.getId()).getBatchInfo();
            for (BatchInfo b : statusList) {
                if (b.getState() == BatchStateEnum.Completed
                        || b.getState() == BatchStateEnum.Failed) {
                    if (incomplete.remove(b.getId())) {
                        System.out.println("BATCH STATUS:\n" + b);
                    }
                }
            }
        }
    }



    /**
     * Create a new job using the Bulk API.
     *
     * @param sobjectType
     *            The object type being loaded, such as "Account"
     * @param connection
     *            BulkConnection used to create the new job.
     * @return The JobInfo for the new job.
     * @throws AsyncApiException
     */
    private JobInfo createJob(String sobjectType, BulkConnection connection)
            throws AsyncApiException {
        JobInfo job = new JobInfo();
        job.setObject(sobjectType);
        job.setOperation(OperationEnum.insert);
        job.setContentType(ContentType.CSV);
        job = connection.createJob(job);
        System.out.println(job);
        return job;
    }



    /**
     * Create the BulkConnection used to call Bulk API operations.
     */
    public BulkConnection getBulkConnection(String userName, String password)
            throws ConnectionException, AsyncApiException {
        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(userName);
        partnerConfig.setPassword(password);
        partnerConfig.setAuthEndpoint("https://login.salesforce.com/services/Soap/u/48.0");
        // Creating the connection automatically handles login and stores
        // the session in partnerConfig
        new PartnerConnection(partnerConfig);
        // When PartnerConnection is instantiated, a login is implicitly
        // executed and, if successful,
        // a valid session is stored in the ConnectorConfig instance.
        // Use this key to initialize a BulkConnection:
        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());
        // The endpoint for the Bulk API service is the same as for the normal
        // SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
        String soapEndpoint = partnerConfig.getServiceEndpoint();
        String apiVersion = "48.0";
        String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/"))
                + "async/" + apiVersion;
        config.setRestEndpoint(restEndpoint);
        // This should only be false when doing debugging.
        config.setCompression(true);
        // Set this to true to see HTTP requests and responses on stdout
        config.setTraceMessage(false);
        BulkConnection connection = new BulkConnection(config);
        return connection;
    }




    private List<BatchInfo> createBatchesFromKafka(BulkConnection connection,
                                                   JobInfo jobInfo, String json)
            throws IOException, AsyncApiException {
        List<BatchInfo> batchInfos = new ArrayList<BatchInfo>();

        // Split the CSV file into multiple batches
        try {
            createBatch(json, batchInfos, connection, jobInfo);

        } finally {
            //Do nothing
        }
        return batchInfos;
    }

    /**
     * Create a batch by uploading the contents of the file.
     * This closes the output stream.
     *
     * @param bulkJson
     *            The string used to write the JSON data for a single batch.
     * @param batchInfos
     *            The batch info for the newly created batch is added to this list.
     * @param connection
     *            The BulkConnection used to create the new batch.
     * @param jobInfo
     *            The JobInfo associated with the new batch.
     */
    private void createBatch(String bulkJson,
                             List<BatchInfo> batchInfos, BulkConnection connection, JobInfo jobInfo)
            throws IOException, AsyncApiException {

        InputStream tmpInputStream = new ByteArrayInputStream(bulkJson.getBytes(StandardCharsets.UTF_8));
        try {
            BatchInfo batchInfo =
                    connection.createBatchFromStream(jobInfo, tmpInputStream);
            System.out.println(batchInfo);
            batchInfos.add(batchInfo);

        } finally {
            tmpInputStream.close();
        }
    }

}
