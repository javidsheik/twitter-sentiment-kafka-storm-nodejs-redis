package storm.demo;

import java.util.HashSet;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import redis.clients.jedis.Jedis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class WordSentimentBolt implements IRichBolt {

	String redisURL;
	public WordSentimentBolt redis(String url) { redisURL = url;return this; }
	public String redis() { return redisURL; }
	
	
	transient OutputCollector collector;
	transient HashSet<String> update;
	transient Jedis jedis;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		update = new HashSet<String>();
		jedis = new Jedis(redisURL);
		
		(new Timer("updater")).schedule(new TimerTask() {

			@Override
			public void run() {
				HashSet<String> words;
				synchronized(update) {
					words = update;
					update = new HashSet<String>();
				}
				
				if(words.size() > 0)
					jedis.publish("TweetSentiment", ""+ words.size());
			}
			
		}, 1000, 1000);
	}

	public void execute(Tuple input) {
		
		Integer line      = input.getIntegerByField("line");
        String  user      = input.getStringByField("user");
		String  text      = input.getStringByField("text");
		Integer sentiment = input.getIntegerByField("sentiment");
		
		 
		jedis.zadd("TweetSentiment", line, "" + sentiment);
		
		for(String word : text.toLowerCase().split("\\s+")) {			
			jedis.zadd("Tweets",line, word);			
			synchronized(update) { update.add(word); }
		}
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
