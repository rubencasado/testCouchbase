package com.meccano.microservices;

import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.meccano.kafka.MessageBody;

import java.util.Dictionary;
import java.util.Hashtable;

/**
 * Created by ruben.casado.tejedor on 01/09/2016.
 */
public class OrderFulfillmentResponse implements com.meccano.kafka.MessageBody{

        public Hashtable<String, Integer> results;

        public OrderFulfillmentResponse(ViewResult result){
            results = new Hashtable<String, Integer>();
            for (ViewRow row : result)
                results.put(row.key().toString(), (Integer)row.value());

        }

}
