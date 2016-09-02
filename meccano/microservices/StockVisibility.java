package com.meccano.microservices;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import com.meccano.utils.CBconfig;
import com.meccano.utils.Pair;

import java.util.ArrayList;

/**
 * Created by ruben.casado.tejedor on 30/08/2016.
 */
public class StockVisibility extends MicroService {


    public StockVisibility(KafkaBroker kafka, CBconfig db){
        super("StockVisibility", kafka, "StockVisibilityRequest",db);

    }
    @Override
    protected void processMessage() {

        //get message from Kafka
        //there is one topic for each possible destination MS
        KafkaMessage kafka_msg= this.kafka.getMessage(this.getTopicSubscription());
        StockVisibilityRequest request= (StockVisibilityRequest)kafka_msg.getMessageBody();

        ArrayList<String> stores = getStores();
        // query CouchBase to know stock of the product in each associated store
        StockVisibilityResponse  stock= new StockVisibilityResponse(request.stock_id);
        for (String store_id: stores) {
            //the document id is store_id-product_id
            String id = store_id +"-"+request.stock_id;
            JsonDocument found = bucket.get(id);
            Integer quantity=  found.content().getInt("quantity");
            if (quantity.intValue()>1)
                stock.add(new Pair<String, Integer>(store_id, quantity));

        }
        //put in kafka the response message
        KafkaMessage msg = new KafkaMessage("StockVisibilityResponse", stock,this.getType(), kafka_msg.getSource());

        this.kafka.putMessage("StockVisibilityResponse", msg);

    }

    @Override
    protected void exit() {
        cluster.disconnect();
    }
    //define the set of stores associated to this MS instance
    protected ArrayList<String> getStores(){
        ArrayList<String> stores = new ArrayList<String> ();
        //mock
        stores.add("Gijon");
        stores.add("Madrid");
        stores.add("Burgos");
        stores.add("Oxford");
        stores.add("Nancy");
        return stores;
    }
}
