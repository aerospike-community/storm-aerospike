package storm.contrib.aerospike.bolt;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.FileOutputStream;
import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * A Bolt for persisting input tuples to Aerospike.
 *
 * @author Reuven Kaswin <Reuven@aerospike.com>
 *
 */

public class HashMapDumpBolt extends BaseRichBolt {
	private OutputCollector collector;

        private static int counter = 0; // TEMP

	/**
	 * @param FileName Name of file to dump out HashMap
	 */
	public HashMapDumpBolt() {
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
	     HashMap theHashMap = (HashMap)(input.getValue(0));
	     Iterator it = theHashMap.entrySet().iterator();
	     while (it.hasNext()) {
	     	Map.Entry kv = (Map.Entry)it.next();
	     	System.out.println(kv.getKey() + " = " + kv.getValue());
	     }
	     this.collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer){
	}


	@Override
	public void cleanup() {
	}
}

