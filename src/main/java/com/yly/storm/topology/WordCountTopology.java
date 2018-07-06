package com.yly.storm.topology;

import com.yly.storm.bolt.SplitSentenceBolt;
import com.yly.storm.bolt.WordCountBolt;
import com.yly.storm.spout.RandomSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * Created by Administrator on 2017/12/28.
 */
public class WordCountTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("RandomSentenceSpout",new RandomSentenceSpout(),2);
        builder.setBolt("SplitSentenceBolt",new SplitSentenceBolt(),5)
                .setNumTasks(10)
                .shuffleGrouping("RandomSentenceSpout");
        builder.setBolt("WordCountBolt",new WordCountBolt(),10)
                .setNumTasks(20)
                .fieldsGrouping("SplitSentenceBolt",new Fields("word"));
        Config config = new Config();


        if(args != null && args.length > 0){
            config.setNumWorkers(3);
            try {
                System.out.print("hahahahah================================");
                System.out.print("hahahahah================================");
                System.out.print("hahahahah================================");
                System.out.print("hahahahah================================");
                System.out.print("hahahahah================================");
                System.out.print("hahahahah================================");
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            }catch (Exception e){

            }
        }else {
            config.setMaxTaskParallelism(20);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCountTopology",config,builder.createTopology());

            Utils.sleep(60000);
            cluster.shutdown();
        }
    }
}
