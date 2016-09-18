package com.meccano.microservices;

import java.util.ArrayList;
import java.util.UUID;

/**
 * Created by ruben.casado.tejedor on 30/08/2016.
 */
public class StockVisibilityRequest implements com.meccano.kafka.MessageBody{

    public StockVisibilityRequest(UUID order_id, ArrayList<String> stock_id) {

        this.stock_id= stock_id;
        this.order_id=order_id;
    }

    public ArrayList<String> stock_id; //array of item_id
    public UUID order_id;
}
