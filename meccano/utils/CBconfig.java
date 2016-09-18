package com.meccano.utils;

import com.couchbase.client.java.CouchbaseCluster;

/**
 * Created by ruben.casado.tejedor on 31/08/2016.
 */
public class CBconfig {

    public CouchbaseCluster cluster;
    public String clusterURL;
    public String bucket;
    public String password;

    public CBconfig(){
        this.clusterURL="localhost";
        this.bucket="default";
        this.password=null;
        this.cluster = CouchbaseCluster.create(this.clusterURL);

    }

    public CBconfig(String clusterURL, String bucket, String pass){
        this.clusterURL=clusterURL;
        this.bucket=bucket;
        this.password=pass;
        this.cluster = CouchbaseCluster.create(this.clusterURL);

    }

}
