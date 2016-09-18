package com.meccano;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseBucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketManager;
import com.meccano.kafka.KafkaBroker;
import com.meccano.microservices.*;
import com.meccano.utils.CBDataGenerator;
import com.meccano.utils.CBconfig;
import org.apache.log4j.Logger;

public class Main {

    static Logger log = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception{

        long init = System.currentTimeMillis();
        log.info("START");



       /* CBDataGenerator generator = new CBDataGenerator(db);
        generator.createItems(500, 100);
        //generator.createOrders(500);
        generator.close();*/

        CBconfig db = new CBconfig();
        db.bucket="mecanno";

        KafkaBroker kafka = new KafkaBroker();
        kafka.createTopic("OrderManagement");
        kafka.createTopic("StockVisibility");
        kafka.createTopic("OrderFulfillment");
        kafka.createTopic("Sourcing");

        Thread orderManagement = new Thread (new OrderManagement(kafka, db, 10000, 100, 20, "out.txt"));
        Thread stockVisibility = new Thread(new StockVisibility(kafka,db));
        Thread orderFulfillment= new Thread (new OrderFulfillment(kafka, db));
        Thread sourcing = new Thread(new SourcingPL(kafka, db));
        //Thread sourcing = new Thread(new SourcingOL(kafka, db));

        orderManagement.start();
        stockVisibility.start();
        orderFulfillment.start();
        sourcing.start();

        orderManagement.join();
        stockVisibility.join();
        orderFulfillment.join();
        sourcing.join();
        log.info("FINISH");
        long total = System.currentTimeMillis() - init;
        log.info("Time: "+ total );


    }

}
