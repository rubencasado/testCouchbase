package com.meccano.utils;

/**
 * Created by ruben.casado.tejedor on 31/08/2016.
 */
public class CBconfig {

    public String cluster;
    public String bucket;
    public String password;

    public CBconfig(){
        this.cluster="localhost";
        this.bucket="default";
        this.password=null;
    }

    public CBconfig(String cluster, String bucket, String pass){
        this.cluster=cluster;
        this.bucket=bucket;
        this.password=pass;
    }

}
