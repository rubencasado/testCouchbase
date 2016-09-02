package com.meccano.microservices;

import java.util.List;

/**
 * Created by ruben.casado.tejedor on 01/09/2016.
 */
public class OrderFulfillmentRequest implements com.meccano.kafka.MessageBody{

    public List<String> stores;
    public String item_id;

    public OrderFulfillmentRequest(String item_id, List<String> stores){
        this.stores=stores;
        this.item_id=item_id;
    }
}
