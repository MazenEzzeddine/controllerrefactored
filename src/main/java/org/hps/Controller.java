package org.hps;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
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


    static Long sleep;
    static String topic;
    static String cluster;
    static Long poll;
    static String BOOTSTRAP_SERVERS;
    static Map<TopicPartition, OffsetAndMetadata> offsets;


    ////WIP TODO
    public static Map<MemberDescription, Float> maxConsumptionRatePerConsumer = new HashMap<>();
    public static Map<MemberDescription, Long> consumerToLag = new HashMap<>();
    public static Instant lastDecision;
    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;
    ///////////////////////////////////////////////////////////////////////////


    static Instant lastUpScaleDecision;
    static Instant lastDownScaleDecision;
    static boolean firstIteration= true;

    static HttpClient client = HttpClient.newHttpClient();



    public static void main(String[] args) throws ExecutionException, InterruptedException {
        readEnvAndCrateAdminClient();
        lastUpScaleDecision = Instant.now();
        lastDownScaleDecision = Instant.now();





        while (true) {
            log.info("New Iteration:");
            getCommittedLatestOffsetsAndLag();
            //computeTotalArrivalRate();

            log.info("Sleeping for {} seconds", sleep / 1000.0);
            Thread.sleep(sleep);


            callPrometheus();
            log.info("End Iteration;");
            log.info("=============================================");
        }


    }


    private static void readEnvAndCrateAdminClient() {
        sleep = Long.valueOf(System.getenv("SLEEP"));
        topic = System.getenv("TOPIC");
        cluster = System.getenv("CLUSTER");
        poll = Long.valueOf(System.getenv("POLL"));
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
                    currentPartitionToCommittedOffset.get(e.getKey()));
            previousPartitionToLastOffset.put(e.getKey(),
                    currentPartitionToLastOffset.get(e.getKey()));



            currentPartitionToCommittedOffset.put(e.getKey(), committedOffset);
            currentPartitionToLastOffset.put(e.getKey(), latestOffset);
            partitionToLag.put(e.getKey(), lag);


        }



        ///////////////////////////////////////////////////

        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(Collections.singletonList(Controller.CONSUMER_GROUP));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();
        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();


        if(!firstIteration){
            computeTotalArrivalRate();
        }else{
            firstIteration = false;
        }
    }


    private static void computeTotalArrivalRate() throws ExecutionException, InterruptedException {
        double totalConsumptionRate;
        double totalArrivalRate;

        long totalpreviouscommittedoffset = 0;
        long totalcurrentcommittedoffset = 0;
        long totalpreviousendoffset = 0;
        long totalcurrentendoffset = 0;
        for (TopicPartition tp : offsets.keySet()) {
            totalpreviouscommittedoffset += previousPartitionToCommittedOffset.get(tp);
            totalcurrentcommittedoffset += currentPartitionToCommittedOffset.get(tp);
            totalpreviousendoffset += previousPartitionToLastOffset.get(tp);
            totalcurrentendoffset += currentPartitionToLastOffset.get(tp);
        }


        totalConsumptionRate = ((double) (totalcurrentcommittedoffset - totalpreviouscommittedoffset) / (double)sleep);
        totalArrivalRate = ((double) (totalcurrentendoffset - totalpreviousendoffset) / (double)sleep);

        log.info("totalArrivalRate {}, totalconsumptionRate {}",
                totalArrivalRate * 1000.0, totalConsumptionRate * 1000.0);

   /*     log.info("time since last up scale decision is {}", Duration.between(lastUpScaleDecision, Instant.now()).toSeconds());
        log.info("time since last down scale decision is {}", Duration.between(lastUpScaleDecision, Instant.now()).toSeconds());*/

       // youMightWanttoScale(totalArrivalRate);

    }


    private static void youMightWanttoScale(double totalArrivalRate) throws ExecutionException, InterruptedException {
        log.info("Inside you youMightWanttoScale");
        int size = consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).members().size();
        log.info("curent group size is {}", size);

        if (Duration.between(lastUpScaleDecision, Instant.now()).toSeconds() >= 30 ) {
            log.info("Upscale logic, Up scale cool down has ended");

            upScaleLogic(totalArrivalRate, size);
        } else {
            log.info("Not checking  upscale logic, Up scale cool down has not ended yet");
        }


        if (Duration.between(lastDownScaleDecision, Instant.now()).toSeconds() >= 60 ) {
            log.info("DownScaling logic, Down scale cool down has ended");
            downScaleLogic(totalArrivalRate, size);
        }else {
            log.info("Not checking  down scale logic, down scale cool down has not ended yet");
        }
    }

    private static void upScaleLogic(double totalArrivalRate, int size) {
        if ((totalArrivalRate * 1000) > size *poll) {
            log.info("Consumers are less than nb partition we can scale");

            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);

                //  firstIteration = true;

                log.info("Since  arrival rate {} is greater than  maximum consumption rate " +
                        "{} ,  I up scaled  by one ", totalArrivalRate * 1000, size * poll);
            }

            lastUpScaleDecision = Instant.now();
            lastDownScaleDecision = Instant.now();
        }
    }




    private static void downScaleLogic(double totalArrivalRate, int size) {
        if ((totalArrivalRate * 1000) < (size - 1) * poll) {

            log.info("since  arrival rate {} is lower than maximum consumption rate " +
                            " with size - 1  I down scaled  by one {}",
                    totalArrivalRate * 1000, size * poll);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
                if (replicas > 1) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                    lastDownScaleDecision = Instant.now();
                    lastUpScaleDecision = Instant.now();

                } else {
                    log.info("Not going to  down scale since replicas already one");
                }
            }
        }
    }



    private static void  callPrometheus() throws InterruptedException {

    String all3 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";


        try {

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(all3))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
               // System.out.println(response.body() + "\n");
                parseJson(response.body());
            } else {
                System.out.println("Error: status = "
                        + response.statusCode()
                        + "\n");
            }

        } catch (IllegalArgumentException | IOException | InterruptedException | URISyntaxException ex) {
            System.out.println("That is not a valid URI.\n");
        }


}


    private static void parseJson(String json) {
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject)jsonObject.get("data");

        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);

        JSONArray jreq = jobj.getJSONArray("value");

        //System.out.println("time stamp: " + jreq.getString(0));
        System.out.println("totalArrivalRate prometheus: " + Double.parseDouble( jreq.getString(1)));


        //System.out.println((System.currentTimeMillis()));

        String ts = jreq.getString(0);
        ts = ts.replace(".", "");
        //TODO attention to the case where after the . there are less less than 3 digits
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
        Date d = new Date(Long.parseLong(ts));
        System.out.println("date to corresponding timestamp : " + sdf.format(d));

       // System.out.println("==================================================");
    }








}










