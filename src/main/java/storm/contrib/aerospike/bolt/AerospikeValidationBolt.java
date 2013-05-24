package storm.contrib.aerospike.bolt;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * A Bolt for persisting input tuples to Aerospike.
 *
 * @author Reuven Kaswin <Reuven@aerospike.com>
 *
 */

public class AerospikeValidationBolt extends BaseRichBolt {
	private OutputCollector collector;
	private HashMap pending; // Map to keep vvalues for fields pending comparison
	private final String KeyName;

        private static int counter = 0; // TEMP

	public AerospikeValidationBolt() {
		this.KeyName = "key";
	}

	public AerospikeValidationBolt(String KeyName) {
		this.KeyName = KeyName;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.pending = new HashMap(1000, 0.75f);
	}

	@Override
	public void execute(Tuple input) {
	     // The tuple is either a Map (record read from DB) or multiple individual values (the data to be inserted)

	     if (input.contains("ASrecordMap")) {
		Map inputData = (Map) (input.getValueByField("ASrecordMap"));
	        if (TupleStoredYet(inputData)) {
			if (CompareValues(inputData)) {
				this.collector.ack(input);
				return;
			} else {
				this.collector.fail(input);
				return;
			}
		}
	        this.collector.ack(input);
		return;
	     } else if (input.contains(this.KeyName)) {
	     	if (TupleStoredYet(input)) {
			if (CompareValues(input)) {
				this.collector.ack(input);
				return;
			} else {
				this.collector.fail(input);
				return;
			}
                } 
	        this.collector.ack(input);
		return;
	     } else {
		this.collector.fail(input);
		return;
	     }
	
/*
	     System.out.println("ACK'd records: " + ++counter);
	     this.collector.ack(input);
*/
	}

	private boolean CompareValues(Map inputRecord) {
		//System.out.println("Compare Record");
		Map savedValues = (Map)(this.pending.get(inputRecord.get(this.KeyName)));
		// Both stream tuples should have the same number of kv pairs
		if (inputRecord.size() != savedValues.size()) {
			this.pending.remove(inputRecord.get(this.KeyName));
			//System.out.println("Size failure");
			return false;
		}
		Iterator it = inputRecord.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry nextValue = (Map.Entry)(it.next());
			if ((((Comparable)(nextValue.getValue())).compareTo(savedValues.get(nextValue.getKey()))) != 0) {
				//System.out.println("Value failure: " + nextValue.getValue() + "/" + savedValues.get(nextValue.getKey()));
				this.pending.remove(inputRecord.get(this.KeyName));
				return false;
			}
		}
		this.pending.remove(inputRecord.get(this.KeyName));
		return true;
	}

	private boolean CompareValues(Tuple input) {
		//System.out.println("Compare Input");
		Map savedValues = (Map)(this.pending.get(input.getValueByField(this.KeyName)));
		// Both stream tuples should have the same number of kv pairs
		if (input.size() != savedValues.size()) {
			this.pending.remove(this.pending.get(this.KeyName));
			//System.out.println("Size failure");
			return false;
		}
		// Get hold of the Map for this Key Value
		for (String field: input.getFields()) {
			if ((((Comparable)(input.getValueByField(field))).compareTo(savedValues.get(field))) != 0) {
				//System.out.println("Value failure: " + input.getValueByField(field) + "/" + savedValues.get(field));
				this.pending.remove(input.getValueByField(this.KeyName));
				return false;
			}
		}
		this.pending.remove(input.getValueByField(this.KeyName));
		return true;
	}
		

	private boolean TupleStoredYet(Map inputRecord) {
		//System.out.println("TupleStoredYet Record");
		// Do we have values for the key stored yet?
		if (this.pending.containsKey(inputRecord.get(this.KeyName))) {
			//System.out.println("TupleStoredYet Record true");
			return true;
		}
		StoreTuple(inputRecord);
		//System.out.println("TupleStoredYet Record false");
		return false;
	}

	private boolean TupleStoredYet(Tuple input) {
		//System.out.println("TupleStoredYet Tuple");
		if (this.pending.containsKey(input.getValueByField(this.KeyName))) {
			//System.out.println("TupleStoredYet Tuple true");
			return true;
		}
		StoreTuple(input);
		//System.out.println("TupleStoredYet Tuple false");
		return false;
	}

	private void StoreTuple(Map FieldsToStore) {
	        //System.out.println("StoreTuple key: " + FieldsToStore.get(this.KeyName));
		this.pending.put(FieldsToStore.get(this.KeyName), FieldsToStore);
	}

	private void StoreTuple(Tuple tupleToStore) {
	     Object inputField;
	     HashMap tupleFields = new HashMap(tupleToStore.size());
             for (String field : tupleToStore.getFields()) {
		tupleFields.put(field, tupleToStore.getValueByField(field));
	     }
	     //System.out.println("StoreTuple key: " + tupleFields.get(this.KeyName));
	     this.pending.put(tupleFields.get(this.KeyName), tupleFields);
	}
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer){
	}


	@Override
	public void cleanup() {
	}
}

