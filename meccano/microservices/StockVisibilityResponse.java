package com.meccano.microservices;

import com.meccano.utils.Pair;
import java.util.ArrayList;

/**
 * Created by ruben.casado.tejedor on 30/08/2016.
 */
public class StockVisibilityResponse implements com.meccano.kafka.MessageBody{

    public StockVisibilityResponse (String stock_id){
        this.stock_id=stock_id;
    }

    protected String stock_id;
    protected ArrayList<Pair<String,Integer>> stocks;
    protected void add(Pair<String,Integer> p){
        stocks.add(p);
    }
}
