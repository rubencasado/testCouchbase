package com.meccano.microservices;

import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import com.meccano.utils.CBconfig;
import com.meccano.utils.Pair;
import org.apache.log4j.Logger;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ruben.casado.tejedor on 08/09/2016.
 */
public class OrderManagement extends MicroService {

    protected int n_orders;
    protected int processed;
    protected int variety;
    protected int frequency;
    protected BufferedWriter file;
    protected Hashtable<UUID, ArrayList<Pair<String, Integer>>> quantities;

    protected Hashtable<UUID, StockVisibilityRequest> stock_requests;
    protected Hashtable<UUID, StockVisibilityResponse> stock_responses;
    protected Hashtable<UUID, OrderFulfillmentRequest> order_request;
    protected Hashtable<UUID, OrderFulfillmentResponse> order_responses;
    protected Hashtable<UUID, SourcingRequest> sourcing_request;
    protected Hashtable<UUID, SourcingResponse> sourcing_responses;

    protected Hashtable<UUID, Long> init_time;
    protected Hashtable<UUID, Long> finish_time;

    static Logger log = Logger.getLogger(OrderManagement.class.getName());


    protected ArrayList<String> getRandomItems(int number, int variety){
        Random rnd= new Random(System.currentTimeMillis());
        ArrayList<String> items = new ArrayList<String>();
        for (int i =0; i< number; i++){
            Integer r= rnd.nextInt(variety)+1;
            if (!items.contains(r.toString()))
                items.add(r.toString());
        }
        return items;
    }

    protected StockVisibilityRequest getStockRequest(UUID id, ArrayList<String> items){
        return new StockVisibilityRequest(id, items);
    }

    protected OrderFulfillmentRequest getOrderRequest(UUID id, ArrayList<String> items){
        return new OrderFulfillmentRequest(id, items, this.getStores());
    }
    protected void processStockResponse(StockVisibilityResponse response){
        this.stock_responses.put(response.order_id, response);
        // Check if OrderManamagent have already received both
        // StockVisibilityResponse and OrderFulfillment response
        if (this.order_responses.get(response.order_id)!= null){
            SourcingRequest body= this.createSourcingRequest(response.order_id, this.quantities.get(response.order_id));
            KafkaMessage msg = new KafkaMessage("Sourcing","SourcingRequest", body, this.getType(), "Sourcing");
            this.kafka.putMessage("Sourcing", msg);
        }

    }

    protected void processOrderResponse(OrderFulfillmentResponse response){
        log.debug(response.order_id+ " Se procesa mensaje OrderFulfillmentResponse");
        this.order_responses.put(response.order_id, response);
        // Check if OrderManamagent have already received both
        // StockVisibilityResponse and OrderFulfillment response

        if (this.stock_responses.get(response.order_id)!= null) {
            SourcingRequest body= this.createSourcingRequest(response.order_id, this.quantities.get(response.order_id));
            KafkaMessage msg = new KafkaMessage("Sourcing","SourcingRequest", body, this.getType(), "Sourcing");
            this.kafka.putMessage("Sourcing", msg);
        }
    }

    protected void processSourcingResponse(SourcingResponse response)  {
        this.finish_time.put(response.order_id, System.currentTimeMillis());

        //log the information
        try {
            //order_id
            this.file.write(response.order_id.toString());
            this.file.write(",");
            //init time
            this.file.write(this.init_time.get(response.order_id).toString());
            this.file.write(",");
            //finish time
            this.file.write(this.finish_time.get(response.order_id).toString());
            this.file.write(",");
            //result
            this.file.write(Boolean.toString(response.success) + "\n");
        }
        catch (Exception e){
            log.error("[ERROR] "+e.toString());
        }

        //update number of order processed
        this.processed++;
        log.info("Number of requested processed:" +this.processed);
        //check if all orders have been processed
        if (this.processed==this.n_orders)
            this.end();

        //delete all information about the order
        this.init_time.remove(response.order_id);
        this.finish_time.remove(response.order_id);
        this.stock_requests.remove(response.order_id);
        this.stock_responses.remove(response.order_id);
        this.order_request.remove(response.order_id);
        this.order_responses.remove(response.order_id);
        this.sourcing_request.remove(response.order_id);
        this.stock_responses.remove(response.order_id);

    }

    protected SourcingRequest createSourcingRequest(UUID order_id, ArrayList<Pair<String, Integer>> quantity){

        StockVisibilityResponse sr=  this.stock_responses.get(order_id);
        OrderFulfillmentResponse or = this.order_responses.get(order_id);

        return new SourcingRequest(order_id,sr, or, quantity);
    }

    //Send kafka message to all microservices to kill them
    public void end(){

        this.exit();
        KafkaMessage message= new KafkaMessage("StockVisibility","Kill",null, null,null);
        this.kafka.putMessage("StockVisibility", message);

        message= new KafkaMessage("OrderFulfillment","Kill",null, null,null);
        this.kafka.putMessage("OrderFulfillment", message);

        message= new KafkaMessage("Sourcing","Kill",null, null,null);
        this.kafka.putMessage("Sourcing", message);

        message= new KafkaMessage("OrderManagement","Kill",null, null,null);
        this.kafka.putMessage("OrderManagement", message);


    }

    protected ArrayList<Pair<String, Integer>> getRandomQuantities(ArrayList<String> items){
        Random r= new Random(System.currentTimeMillis());
        ArrayList<Pair<String, Integer>> result = new ArrayList<Pair<String, Integer>>();
        for (int i=0; i< items.size();i++)
            result.add(new Pair(items.get(i), r.nextInt(3)+1));
        return result;
    }

    public void run(){

        //create the orders request
        for (int i=0; i<this.n_orders;i++){

            //generate a new order_id
            UUID order_id = UUID.randomUUID();

            //generate the order items [always 3 items per order]
            ArrayList<String> items= this.getRandomItems(3,this.variety);
            // generate quantity for each item
            this.quantities.put(order_id,this.getRandomQuantities(items));

            //order init time
            this.init_time.put(order_id, System.currentTimeMillis());

            //create the StockVisibility request and send message to Kafka
            StockVisibilityRequest sr = this.getStockRequest(order_id, items);
            this.stock_requests.put(sr.order_id, sr);
            KafkaMessage message = new KafkaMessage("StockVisibility","StockVisibilityRequest",sr, this.getType(),"StockVisibility");
            this.kafka.putMessage("StockVisibility", message);

            //create the OrderFulfillment request and send message to Kafka
            OrderFulfillmentRequest or= this.getOrderRequest(order_id, items);
            this.order_request.put(or.order_id, or);
            message = new KafkaMessage("OrderFulfillment","OrderFulfillmentRequest",or, this.getType(),"OrderFulfillment");
            this.kafka.putMessage("OrderFulfillment", message);


            try {
                Thread.sleep(frequency);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //process intermediate results until the end of the order responses
        while (!this.finish){
            KafkaMessage message = consumMessage();

            if (message!=null){
                log.debug("Mensaje a procesar:"+message.getType());
                if (message.getType()=="StockVisibilityResponse")
                    this.processStockResponse((StockVisibilityResponse)message.getMessageBody());
                else if (message.getType()=="OrderFulfillmentResponse")
                    this.processOrderResponse((OrderFulfillmentResponse) message.getMessageBody());
                else if (message.getType()=="SourcingResponse")
                    this.processSourcingResponse((SourcingResponse) message.getMessageBody());
                else if (message.getType()=="Kill")
                    this.finish=true;
                else
                    System.err.println("[ERROR] OrderManagemnet: unknow message "+message.getType());
            }
        }

    }

    public OrderManagement(KafkaBroker kafka, CBconfig db, int n_orders, int variety, int frecuency, String path) throws IOException {
        super ("OrderManagement", kafka, "OrderManagement", db);
        this.n_orders=n_orders;
        this.processed=0;
        this.variety = variety;
        this.frequency=frecuency;
        this.file = new BufferedWriter(new FileWriter(new File(path).getAbsoluteFile()));
        this.quantities = new  Hashtable<UUID, ArrayList<Pair<String, Integer>>>();

        this.stock_requests = new  Hashtable<UUID, StockVisibilityRequest> ();
        this.stock_responses = new  Hashtable<UUID, StockVisibilityResponse> ();

        this.order_request = new Hashtable<UUID, OrderFulfillmentRequest> ();
        this.order_responses= new  Hashtable<UUID, OrderFulfillmentResponse> ();
        this.sourcing_request =  new Hashtable<UUID, SourcingRequest> ();
        this.sourcing_responses = new  Hashtable<UUID, SourcingResponse>();

        this.init_time= new  Hashtable<UUID, Long> ();
        this.finish_time= new Hashtable<UUID, Long> ();


    }

    //method from MicroService class. Not used here since run() has been re-implemented
    protected void processMessage(KafkaMessage message) {

    }

        protected void exit() {

            try {
                this.bucket.close();
                this.cluster.disconnect();
                this.file.close();
                log.info("OrderManagement exit");

            } catch (Exception e) {
                log.error(e.toString());
            }
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
