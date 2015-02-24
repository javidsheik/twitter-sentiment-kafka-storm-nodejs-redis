package storm.demo;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

public class SentimentAnalysisBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8131515295719242137L;
	public static final String SENTIMENT_FILE_NAME = "sentiment.txt";
	public static final String ACRONYM_FILE_NAME = "acronyms.txt";
	public static final String EMICON_FILE_NAME = "emicons.txt";
	
	transient OutputCollector collector;
	
	private SortedMap<String,Integer> aSentimentMap = null;
	private SortedMap<String,String> aAcronymsMap = null;
	private SortedMap<String,Integer> aEmiconsMap = null;
	
	public void prepare(final Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		aSentimentMap = Maps.newTreeMap();
		aAcronymsMap = Maps.newTreeMap();
		aEmiconsMap = Maps.newTreeMap();
		
		try {
			final URL url = Resources.getResource(SENTIMENT_FILE_NAME);
			final URL eurl = Resources.getResource(EMICON_FILE_NAME);
			final URL aurl = Resources.getResource(ACRONYM_FILE_NAME);
			
			//Load Tokens Sentiments
			
			String text = Resources.toString(url, Charsets.UTF_8);
			Iterable<String> lineSplit = Splitter.on("\n").trimResults().omitEmptyStrings().split(text);
			List<String> tabSplit;
			for (final String str: lineSplit) {
				tabSplit = Lists.newArrayList(Splitter.on("\t").trimResults().omitEmptyStrings().split(str));
				aSentimentMap.put(tabSplit.get(0), Integer.parseInt(tabSplit.get(1)));
			}

			//Load Emicons Sentiments
			
			text = Resources.toString(eurl, Charsets.UTF_8);
			lineSplit = Splitter.on("\n").trimResults().omitEmptyStrings().split(text);
			
			for (final String str: lineSplit) {
				tabSplit = Lists.newArrayList(Splitter.on("\t").trimResults().omitEmptyStrings().split(str));
				aEmiconsMap.put(tabSplit.get(0), Integer.parseInt(tabSplit.get(1)));
			}
			
			//Load Acronyms 
			
			text = Resources.toString(aurl, Charsets.UTF_8);
			lineSplit = Splitter.on("\n").trimResults().omitEmptyStrings().split(text);
			
			for (final String str: lineSplit) {
				tabSplit = Lists.newArrayList(Splitter.on("|").trimResults().omitEmptyStrings().split(str));
				aAcronymsMap.put(tabSplit.get(0), tabSplit.get(1));
			}
			
		} catch (final IOException ioException) {
			ioException.printStackTrace();
			System.exit(1);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line","user","text","sentiment"));
	}

	public void execute(Tuple input) {
			String[] parts = input.getStringByField("message").split("\t");
			
			Integer line = Integer.parseInt(parts[0]);
			String  user = parts[1];
			String  text    = parts[2].replaceAll("\\p{Punct}|\\d"," ");
			
			final Iterable<String> words = Splitter.on(' ')
                    .trimResults()
                    .omitEmptyStrings()
                    .split(text);
			
			int sentiment = 0;
			
			//Loop thru all the words and find the sentiment of this tweet.
			
			for (final String word : words) {
				
				//Check for acronyms and replace the tokens.
				
				if(aAcronymsMap.containsKey(word)){
					text = aAcronymsMap.get(word);
				}
				
				if(aSentimentMap.containsKey(word)){
					sentiment += aSentimentMap.get(word);
				}
				
				if(aEmiconsMap.containsKey(word)){
					sentiment += aEmiconsMap.get(word);
				}
			}
			
			collector.emit(new Values(line,user,text,sentiment));
			collector.ack(input);
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}


	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
