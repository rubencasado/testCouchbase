package com.meccano.microservices;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by ruben.casado.tejedor on 01/09/2016.
 */
public class OrderFulfillmentRequest implements com.meccano.kafka.MessageBody{

    public List<String> stores;
    public ArrayList<String> item_id;
    public UUID order_id;

    public OrderFulfillmentRequest(UUID order_id, ArrayList<String> item_id, List<String> stores){
        this.stores=stores;
        this.item_id=item_id;
        this.order_id=order_id;
    }
}
