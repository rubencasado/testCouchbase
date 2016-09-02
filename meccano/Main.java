package com.meccano;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseBucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketManager;
import com.meccano.utils.CBDataGenerator;
import com.meccano.utils.CBconfig;

public class Main {

    public static void main(String[] args) {

        CBconfig db = new CBconfig();
        db.bucket="mecanno";
        /*CouchbaseCluster cluster = CouchbaseCluster.create(db.cluster);
        Bucket bucket = cluster.openBucket(db.bucket);
        BucketManager manager = bucket.bucketManager();
        manager.flush();

        */
        CBDataGenerator generator = new CBDataGenerator(db);

        generator.createItems(200, 50);
        generator.createOrders(500);

        generator.close();
    }
}
