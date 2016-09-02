package com.meccano.microservices;

/**
 * Created by ruben.casado.tejedor on 30/08/2016.
 */
public class StockVisibilityRequest implements com.meccano.kafka.MessageBody{

    public StockVisibilityRequest(String stock_id) {
        this.stock_id= stock_id;
    }

    public String stock_id;
}
