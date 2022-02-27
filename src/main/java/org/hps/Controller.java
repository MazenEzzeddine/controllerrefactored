package org.hps;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Controller {
    private static final Logger log = LogManager.getLogger(Controller.class);

    public static String CONSUMER_GROUP;
    public static int numberOfPartitions;
    public static AdminClient admin = null;
    public static Map<TopicPartition, Long> currentPartitionToCommittedOffset = new HashMap<>();
    public static Map<TopicPartition, Long> previousPartitionToCommittedOffset = new HashMap<>();
    public static Map<TopicPartition, Long> previousPartitionToLastOffset = new HashMap<>();
    public static Map<TopicPartition, Long> currentPartitionToLastOffset = new HashMap<>();
    public static Map<TopicPartition, Long> partitionToLag = new HashMap<>();

    public static String mode;
    static boolean firstIteration = true;
    static Long sleep;
    static Long waitingTime;
    static String topic;
    static String cluster;
    static Long poll;
    static Long SEC;
    static String choice;
    static String BOOTSTRAP_SERVERS;
    static Long allowableLag;
    static Map<TopicPartition, OffsetAndMetadata> offsets;


    ////WIP TODO
    public static Map<MemberDescription, Float> maxConsumptionRatePerConsumer = new HashMap<>();
    public static Map<MemberDescription, Long> consumerToLag = new HashMap<>();
    public static Instant lastDecision;
    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;
    ///////////////////////////////////////////////////////////////////////////



    public static void main(String[] args) throws ExecutionException, InterruptedException {
        readEnvAndCrateAdminClient();

        while (true) {
            getCommittedLatestOffsetsAndLag();
            computeTotalArrivalRate();
            Thread.sleep(sleep);
        }


    }


    private static void readEnvAndCrateAdminClient(){
        sleep = Long.valueOf(System.getenv("SLEEP"));
        topic = System.getenv("TOPIC");
        cluster = System.getenv("CLUSTER");
        //poll = Long.valueOf(System.getenv("POLL"));
        CONSUMER_GROUP = System.getenv("CONSUMER_GROUP");
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);

    }



    private static void getCommittedLatestOffsetsAndLag() throws ExecutionException, InterruptedException {
        //get committed  offsets
     offsets = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                        .partitionsToOffsetAndMetadata().get();
        numberOfPartitions = offsets.size();
        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        //initialize consumer to lag to 0
        for (TopicPartition tp : offsets.keySet()) {
            requestLatestOffsets.put(tp, OffsetSpec.latest());
            partitionToLag.put(tp, 0L);
        }


        //blocking call to query latest offset
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();

        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
            long committedOffset = e.getValue().offset();
            long latestOffset = latestOffsets.get(e.getKey()).offset();
            long lag = latestOffset - committedOffset;

            previousPartitionToCommittedOffset.put(e.getKey(),
                    currentPartitionToCommittedOffset.getOrDefault(e.getKey(),0L));

            previousPartitionToLastOffset.put(e.getKey(),
                    currentPartitionToLastOffset.getOrDefault(e.getKey(), 0L));

            currentPartitionToCommittedOffset.put(e.getKey(), committedOffset);
            currentPartitionToLastOffset.put(e.getKey(), latestOffset);
            partitionToLag.put(e.getKey(), lag);
        }
    }


    private static void computeTotalArrivalRate(){
        double totalConsumptionRate = 0;
        double totalArrivalRate = 0;

        long totalpreviouscommittedoffset = 0;
        long totalcurrentcommittedoffset = 0;
        long totalpreviousendoffset = 0;
        long totalcurrentendoffset = 0;
        for(TopicPartition tp: offsets.keySet()) {
            totalpreviouscommittedoffset += previousPartitionToCommittedOffset.get(tp);
            totalcurrentcommittedoffset += currentPartitionToCommittedOffset.get(tp);
            totalpreviousendoffset       +=   previousPartitionToLastOffset.get(tp);
            totalcurrentendoffset += currentPartitionToLastOffset.get(tp);
        }


        totalConsumptionRate = (double) (totalcurrentcommittedoffset - totalpreviouscommittedoffset) / sleep;
        totalArrivalRate = (double) (totalcurrentendoffset - totalpreviousendoffset) / sleep;

        log.info("totalArrivalRate {}, totalconsumptionRate {}",
                totalArrivalRate * 1000, totalConsumptionRate * 1000);


    }



}




