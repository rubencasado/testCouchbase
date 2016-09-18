package com.meccano.microservices;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.meccano.kafka.MessageBody;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by ruben.casado.tejedor on 01/09/2016.
 */
public class OrderFulfillmentResponse implements com.meccano.kafka.MessageBody{

        public Hashtable<String, Hashtable<String,Integer>> results; // <item_id, <store_id-item_id, quantity>>
        public UUID order_id;

        static Logger log = Logger.getLogger(OrderFulfillmentResponse.class.getName());


    public OrderFulfillmentResponse(UUID order_id, Hashtable<String,List<ViewRow>> result){
            this.order_id=order_id;
            results = new Hashtable<String, Hashtable<String,Integer>>();
            Enumeration<String> itr = result.keys();
            while (itr.hasMoreElements()){
                String key = itr.nextElement();
                List<ViewRow> rows = result.get(key);
                Hashtable<String, Integer> temp = new Hashtable<String, Integer>();
                for (ViewRow row : rows) {
                    JsonArray j = (JsonArray)row.key();
                    temp.put(j.getString(1), (Integer) row.value());
                }
                results.put(key, temp);
            }


        }

}
