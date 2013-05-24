package storm.contrib.aerospike.bolt;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;

import backtype.storm.Config;
//import backtype.storm.LocalCluster;
//import backtype.storm.testing.FeederSpout;
//import backtype.storm.topology.TopologyBuilder;
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

public class AerospikeEnrichBolt extends BaseRichBolt {
	private OutputCollector collector;

	private AerospikeClient AerospikeClient;
	private QueryPolicy AerospikeQueryPolicy;
	private final String AerospikeHost;
	private final int AerospikePort;
	private final String AerospikeNamespace;
	private final String AerospikeSet;
	private final String AerospikeKeyName;
        private static int counter = 0; // TEMP

	/**
	 * @param AerospikeHost Any host in the Aerospike cluster
	 * @param AerospikePort The Aerospike listener port
	 * @param AerospikeNameSpace The Aerospike Namespace to persist to
	 * @param AerospikeSet The Aerospike Set to persist to
	 */
	public AerospikeEnrichBolt(String AerospikeHost, int AerospikePort, String AerospikeNamespace, String AerospikeSet) {
		this.AerospikeHost = AerospikeHost;
		this.AerospikePort = AerospikePort;
		this.AerospikeNamespace = AerospikeNamespace;
		this.AerospikeSet = AerospikeSet;
		this.AerospikeKeyName = "key"; // The default name the maps to the key value
	}
	
	/**
	 * @param AerospikeHost Any host in the Aerospike cluster
	 * @param AerospikePort The Aerospike listener port
	 * @param AerospikeNameSpace The Aerospike Namespace to persist to
	 * @param AerospikeSet The Aerospike Set to persist to
	 * @param AerospikeKeyName The name that maps to the key value
	 */
	public AerospikeEnrichBolt(String AerospikeHost, int AerospikePort, String AerospikeNamespace, String AerospikeSet, String AerospikeKeyName) {
		this.AerospikeHost = AerospikeHost;
		this.AerospikePort = AerospikePort;
		this.AerospikeNamespace = AerospikeNamespace;
		this.AerospikeSet = AerospikeSet;
		this.AerospikeKeyName = AerospikeKeyName;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
		this.collector = collector;
		try {
			this.AerospikeClient = new AerospikeClient(this.AerospikeHost, this.AerospikePort);
			this.AerospikeQueryPolicy = new QueryPolicy();
			this.AerospikeQueryPolicy.maxRetries=10;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/*
 	 * I am only accepting an int or string as the key value for now
 	 */
	@Override
	public void execute(Tuple input) {
	     int BinIndex = 0;
	     Key AerospikeKey;
	     Record ReadRecord;
	     Map ResultsMap;
	     Object AerospikeKeyValue;

	     // First we need to get our key value
	     if (!input.contains(this.AerospikeKeyName)) {
		// Need to raise an exception since the KeyName isn't in the Tuple
		throw new RuntimeException();
	     }
	     // Get the key value from the input Tuple
	     AerospikeKeyValue = input.getValueByField(this.AerospikeKeyName);
	     try {
		// Let's read the record
	        AerospikeKey = new Key(this.AerospikeNamespace, this.AerospikeSet, AerospikeKeyValue);
	     	ReadRecord = AerospikeClient.get(this.AerospikeQueryPolicy, AerospikeKey);
	     } catch (AerospikeException asE) {
	     		throw new RuntimeException(asE);
	     }
	     // If the record doesn't exist, need to create our own Map
	     if (ReadRecord == null) {
		ResultsMap = new HashMap(1);
	     }
	     else {
		// otherwise the Map is the return value from Aerospike
	     	ResultsMap = ReadRecord.bins;
	     }
	     // Add the key to the Map
	     ResultsMap.put(this.AerospikeKeyName, AerospikeKeyValue);
	     this.collector.emit(input, new Values(AerospikeKeyValue, ResultsMap));

	     this.collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer){
	// The output will have to be a Map, since each record may have different bins (fields) in it.
	// Let's call it AS_record
		declarer.declare(new Fields("key", "ASrecordMap"));
	}


	@Override
	public void cleanup() {
		AerospikeClient.close();
	}
}

