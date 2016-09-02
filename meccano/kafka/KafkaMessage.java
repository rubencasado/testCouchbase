package com.meccano.kafka;

/**
 * Created by ruben.casado.tejedor on 30/08/2016.
 * Base class for implementing the messages exchanged by the MS using Kafka
 */
public class KafkaMessage {
    protected String  type;
    protected MessageBody body;
    protected String source;
    protected String destination;

    public KafkaMessage(String type, MessageBody body, String source, String destination) {
        this.type = type;
        this.source=source;
        this.destination=destination;
        this.body=body;
    }

    public String getType(){
        return this.type;
    }
    public MessageBody getMessageBody (){
        return this.body;
    }
    public String getSource(){ return this.source;}
}
