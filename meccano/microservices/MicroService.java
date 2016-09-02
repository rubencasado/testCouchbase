package com.meccano.microservices;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.meccano.kafka.KafkaBroker;
import com.meccano.utils.CBconfig;

import java.util.UUID;

/**
 * Created by ruben.casado.tejedor on 30/08/2016.
 *
 * Base class for simulating the MS behaviour using a thread
 * Use a common KafkaBroker to exchange messages with other MS
 */
public abstract class MicroService implements Runnable {

    protected String type;
    protected UUID instance;
    protected KafkaBroker kafka;
    protected String topic_subscription;
    protected CBconfig db;
    protected boolean finish;

    protected CouchbaseCluster cluster;
    protected Bucket bucket;

    public MicroService(String type, KafkaBroker kafka, String topic, CBconfig db){
        this.type=type;
        this.instance = UUID.randomUUID();
        this.topic_subscription=topic;

        if (db!=null) {
            this.db=db;
            this.finish = false;
        }
        else{
            System.err.println("[ERROR] MS "+type+" generation: CBconfig is null");
            this.finish=true;
            return;
        }

        if (kafka!=null) {
            this.kafka = kafka;
            this.finish = false;
        }
        else{
            System.err.println("[ERROR] MS "+type+" generation: Kafka is null");
            this.finish=true;
            return;
        }
        // Create a cluster reference
        cluster = CouchbaseCluster.create(db.cluster);
        // Connect to the bucket and open it
        if (db.password!=null)
            bucket = cluster.openBucket(db.bucket,db.password);
        else
            bucket = cluster.openBucket(db.bucket);

    }
    public String getType(){
        return this.type;
    }
    public String getTopicSubscription(){
        return this.topic_subscription;
    }
    public UUID getInstance() {
        return this.instance;
    }
    public String getID(){
        return type +"-"+instance.toString();
    }

    public void run(){
        while (!finish){
            this.processMessage();
        }
        exit();
    }
    protected abstract void processMessage();
    protected abstract void exit();
}
