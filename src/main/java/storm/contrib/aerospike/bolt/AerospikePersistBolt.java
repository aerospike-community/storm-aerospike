package storm.contrib.aerospike.bolt;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.RecordExistsAction;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
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

public class AerospikePersistBolt extends BaseRichBolt {
	private OutputCollector collector;

	private AerospikeClient AerospikeClient;
	private WritePolicy AerospikeWritePolicy;
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
	public AerospikePersistBolt(String AerospikeHost, int AerospikePort, String AerospikeNamespace, String AerospikeSet) {
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
	public AerospikePersistBolt(String AerospikeHost, int AerospikePort, String AerospikeNamespace, String AerospikeSet, String AerospikeKeyName) {
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
			this.AerospikeWritePolicy = new WritePolicy();
			this.AerospikeWritePolicy.maxRetries=10;
			this.AerospikeWritePolicy.recordExistsAction = RecordExistsAction.UPDATE;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/*
 	 * I am only accepting an int or string as the key value for now
 	 */
	@Override
	public void execute(Tuple input) {
/*
	     if (true) {
		if ((counter++ % 100000) == 0) {
			System.out.println("records received: " + counter);
		}
		this.collector.ack(input);
		return;
	     }
*/
	     int BinIndex = 0;
	     Object inputField;
	     Key AerospikeKey;
	     Bin[] AerospikeBins = new Bin[input.size() - 1]; // One value is the key, so subtract
	     // First we need to get our key value
	     if (!input.contains(AerospikeKeyName)) {
		// Need to raise an exception since the KeyName isn't in the Tuple
		throw new RuntimeException();
	     }
	     
	     try {
		     inputField = input.getValueByField(this.AerospikeKeyName);
		     AerospikeKey = new Key(this.AerospikeNamespace, this.AerospikeSet, GetAerospikeValue(inputField));
		     // System.out.println("Key : " + input.getValueByField(this.AerospikeKeyName));
	     } catch (AerospikeException asE) {
	     		throw new RuntimeException(asE);
	     }
	     
             for (String field : input.getFields()) {
		if (field.equals(AerospikeKeyName)) {
			continue; // Skip the key value. Maybe this should be optional
		}
	        inputField = input.getValueByField(field);
		AerospikeBins[BinIndex++] = new Bin(field, GetAerospikeValue(inputField));
	     }
	     // Now its time to create/update the record in the Aerospike DB
	     try { 	
		     AerospikeClient.put(this.AerospikeWritePolicy, AerospikeKey, AerospikeBins);
	     } catch (AerospikeException asE) {
	     		throw new RuntimeException(asE);
	     }
	     this.collector.emit(input, new Values(input.getValueByField(this.AerospikeKeyName)));
	     this.collector.ack(input);

	}

	private Object GetAerospikeValue(Object KeyValue) {
		return (KeyValue);
	}	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declare(new Fields(this.AerospikeKeyName));
	}


	@Override
	public void cleanup() {
		AerospikeClient.close();
	}
}

