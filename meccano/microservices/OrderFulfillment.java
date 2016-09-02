package com.meccano.microservices;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import com.meccano.utils.CBconfig;

import java.util.ArrayList;

/**
 * Created by ruben.casado.tejedor on 31/08/2016.
 */
public class OrderFulfillment extends MicroService {

    public OrderFulfillment (KafkaBroker kafka, CBconfig db){
        super("OrderFulfillment", kafka, "OrderFulfillmentRequest",db);
    }

    protected void processMessage() {
        KafkaMessage kafka_msg= this.kafka.getMessage(this.getTopicSubscription());
        OrderFulfillmentRequest request= (OrderFulfillmentRequest)kafka_msg.getMessageBody();

        JsonArray keys =JsonArray.create();
        for (String s: request.stores){
            //["item_id","store_id"]
            keys.add("[\""+request.item_id+"\",\""+s+"\"]");
        }
        ViewQuery query= ViewQuery.from(this.db.bucket,"allocations").keys(keys);
        ViewResult result = this.bucket.query(query);
        int sum=0;
        Integer quantity;

        OrderFulfillmentResponse body = new OrderFulfillmentResponse(result);
        //put in kafka the response message
        KafkaMessage msg = new KafkaMessage("OrderFulfillmentResponse", body, this.getType(), kafka_msg.getSource());
        this.kafka.putMessage("OrderFulfillmentResponse", msg);
    }

    protected void exit() {
        this.cluster.disconnect();
    }

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
