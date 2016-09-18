package com.meccano.kafka;

import com.meccano.microservices.MicroService;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ruben.casado.tejedor on 30/08/2016.
 *
 * Simulates the behaviour of a Kakfa cluster with multiple topics.
 *
 */
public class KafkaBroker {

    protected ArrayList<KafkaTopic> topics;
    static Logger log = Logger.getLogger(KafkaBroker.class.getName());


    public KafkaBroker(){
        topics = new ArrayList<KafkaTopic>();
    }

    public KafkaBroker(List<String> names){
        Iterator<String> itr = names.iterator();
        while (itr.hasNext()){
            topics.add(new KafkaTopic(itr.next()));
        }
    }

    public void createTopic (String name){
        boolean exist= false;
        KafkaTopic topic;
        Iterator<KafkaTopic> itr = topics.iterator();
        while (itr.hasNext()) {
            topic = itr.next();
            if (topic.getName().equals(name))
                exist = true;
        }
        if (!exist)
            topics.add(new KafkaTopic(name));
    }

    public ArrayList<String> getTopicsName(){
        ArrayList<String> names = new ArrayList<String>();
        Iterator<KafkaTopic> itr = topics.iterator();
        while (itr.hasNext()){
            names.add(itr.next().getName());
        }
        return names;
    }


    protected KafkaTopic getTopic(String name){
        Iterator<KafkaTopic> itr = topics.iterator();
        KafkaTopic topic;
        while (itr.hasNext()) {
            topic = itr.next();
            if (topic.getName().equals(name))
                return topic;
        }
        return null;
    }
    public KafkaMessage getMessage(String name){
        KafkaTopic topic = this.getTopic(name);
        if (topic != null){
            if (topic.size()>0)
                return topic.get();
            else
                return null;
        }

        else {
            log.error("[ERROR] KakfaBroker - "+ name+" KakfaTopic is null");
            return null;
        }
    }

    public void putMessage(String name, KafkaMessage msg){
        KafkaTopic topic = this.getTopic(name);
        if (topic!=null)
            topic.put(msg);
    }

    public void removeTopic(String name){
        Iterator<KafkaTopic> itr = topics.iterator();
        KafkaTopic topic;
        while (itr.hasNext()) {
            topic = itr.next();
            if (topic.getName().equals(name)) {
                itr.remove();
                break;
            }
        }

    }

    public int topicSize(String name){
        KafkaTopic topic = this.getTopic(name);
        if (topic != null)
            return topic.size();
        return -1;
    }

    public int totalSize(){
        int total=0;
        Iterator<KafkaTopic> itr = topics.iterator();
        KafkaTopic topic;
        while (itr.hasNext()) {
            topic = itr.next();
            total+=topic.size();
        }
        return total;
    }

}
