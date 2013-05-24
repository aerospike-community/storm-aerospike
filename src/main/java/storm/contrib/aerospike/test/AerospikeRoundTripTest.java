import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import storm.contrib.aerospike.bolt.AerospikePersistBolt;
import storm.contrib.aerospike.bolt.AerospikeEnrichBolt;
import storm.contrib.aerospike.bolt.AerospikeValidationBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.StormSubmitter;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Random;


/**
 *
 * @author Reuven Kaswin <Reuven@aerospike.com>
 *
 */


public class AerospikeRoundTripTest {
	static Object LockObj = new Object();

	public static void main(String[] args) throws Exception {
		LocalCluster cluster = null;
		String host = "127.0.0.1";
		int iterations = 1;
		int port = 3000;
		String nameSpace = "test";
		String set = "bolt";
		int persistThreads = 1;
		int enrichThreads = 1;
		int validateThreads = 1;
		boolean validatePersist = false;
		boolean validateOnly = false;
		boolean submitTopology = false;
		int loopLimit = 0;
		int randomSeed = 1;
		int stormWorkers = 4;
		Options options = new Options();
	
		options.addOption("help", false, "help");
		options.addOption("h", true, "Aerospike host (default 127.0.0.1)");
		options.addOption("p", true, "Aerospike port (default 3000)");
		options.addOption("n", true, "Aerospike namespace (default test)");
		options.addOption("s", true, "Aerospike set (default bolt)");
		options.addOption("seed", true, "Random field seed value (default 1)");
		options.addOption("r", true,
				"number of records(streams) to submit. 0 = infinite (default 1)");
		options.addOption("l", true,
				"highest key value before looping back to 0. 0 = don't loop (default 0)");
		options.addOption("v", false,
				"validate the insertions by reading and comparing (default false)");
		options.addOption("vo", false,
				"validate by reading and comparing to expected values (default false)");
		options.addOption("pt", true,
				"number of persist Bolt threads (default 1)");
		options.addOption("et", true,
				"number of enrich Bolt threads (default 1)");
		options.addOption("vt", true,
				"number of validate Bolt threads (default 1)");
		options.addOption("w", true,
				"number of storm workers (default 4)");
		options.addOption("dbg", false,
				"turn on Topology debugging (default false)");
		options.addOption("st", false,
				"submit Topology to a Storm cluster (default false)");

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException ex) {
			System.out.println("Unexpected exception: " + ex.getMessage());
			System.exit(1);
		}
		if (cmd.hasOption("help")) {
			HelpFormatter formatter = new HelpFormatter();

			formatter.printHelp("java AerospikeRoundTripTest", options);
			System.exit(0);
		}
		if (cmd.hasOption("h")) {
			host = cmd.getOptionValue("h");
		}
		if (cmd.hasOption("p")) {
			port = Integer.parseInt(cmd.getOptionValue("p"));
		}
		if (cmd.hasOption("n")) {
			nameSpace = cmd.getOptionValue("n");
		}
		if (cmd.hasOption("s")) {
			set = cmd.getOptionValue("s");
		}
		if (cmd.hasOption("r")) {
			iterations = Integer.parseInt(cmd.getOptionValue("r"));
		}
		if (cmd.hasOption("w")) {
			stormWorkers = Integer.parseInt(cmd.getOptionValue("w"));
		}
		if (cmd.hasOption("l")) {
			loopLimit = Integer.parseInt(cmd.getOptionValue("l"));
		}
		if (cmd.hasOption("pt")) {
			persistThreads = Integer.parseInt(cmd.getOptionValue("pt"));
		}
		if (cmd.hasOption("v")) {
			validatePersist = true;
		}
		if (cmd.hasOption("vo")) {
			validateOnly = true;
		}
		if (cmd.hasOption("st")) {
			submitTopology = true;
		}
		if (cmd.hasOption("seed")) {
			randomSeed = Integer.parseInt(cmd.getOptionValue("seed"));
		}
		if (cmd.hasOption("et")) {
			enrichThreads = Integer.parseInt(cmd.getOptionValue("et"));
		}
		if (cmd.hasOption("vt")) {
			validateThreads = Integer.parseInt(cmd.getOptionValue("vt"));
		}
		if (validateOnly & validatePersist) {
			System.out.println("Illegal combination of parameters");
			System.exit(1);
		}

		AerospikeTestFeederSpout testSpout = new AerospikeTestFeederSpout(
				randomSeed, iterations, loopLimit,
				new Fields("key", "gender", "ranInt"));

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("testSpout", testSpout);
		if (!validateOnly) {
			builder.setBolt("ASpersist", new AerospikePersistBolt(host, port, nameSpace, set), persistThreads).shuffleGrouping(
					"testSpout");
		}
		if (validatePersist | validateOnly) {
			builder.setBolt("ASenrich", new AerospikeEnrichBolt(host, port, nameSpace, set), enrichThreads).shuffleGrouping(
					validatePersist ? "ASpersist" : "testSpout");
			builder.setBolt("ASvalidate", new AerospikeValidationBolt(), validateThreads).fieldsGrouping("testSpout", new Fields("key")).fieldsGrouping(
					"ASenrich", new Fields("key"));
		}

		Config conf = new Config();

		if (cmd.hasOption("dbg")) {
			conf.setDebug(true);
		}

		conf.setMaxSpoutPending(10000);
		conf.setNumWorkers(stormWorkers);
		if (submitTopology) {
			StormSubmitter.submitTopology("Aerospike-round-trip-example", conf,
					builder.createTopology());
		} else {
			cluster = new LocalCluster();
			cluster.submitTopology("Aerospike-round-trip-example", conf,
					builder.createTopology());
		}

		long startTime = System.currentTimeMillis();

		synchronized (LockObj) {
			try {
				LockObj.wait();
			} catch (InterruptedException ex) {}
		}
		long endTime = System.currentTimeMillis();
	
		testSpout.close();
		if (!submitTopology) {
			cluster.shutdown();
		}
		System.out.println(
				"Total Execution Time: " + (endTime - startTime) + " milliseconds");
		System.out.println(
				"Tuples per second: " + iterations / ((endTime - startTime) / 1000));
	
	}

	static class AerospikeTestFeederSpout extends FeederSpout {
		int streamsProcessed = 0;
		int submittedCount = 0;
		int failureCount = 0;
		int highestKeyValue = 0;
		int keyValue = 0;
		int streamsToProcess;
		Random ranGen;

		AerospikeTestFeederSpout(int ranSeed, int records, int maxKey, Fields outFields) {
			super(outFields);
			streamsToProcess = records;
			highestKeyValue = maxKey;
			ranGen = new Random(ranSeed);
		}

		@Override
		public void ack(Object msgId) {
			streamProcessed();
		}

		@Override
		public void fail(Object msgId) {
			// System.out.println("Failure: " + msgId);
			failureCount++;
			streamProcessed();
		}

		void streamProcessed() {
			streamsProcessed++;
			if ((streamsToProcess > 0) && (streamsProcessed >= streamsToProcess)) {
				System.out.println("");
				System.out.println("Processed " + streamsProcessed + " tuples");
				if (failureCount > 0) {
					System.out.println(
							"Number of failed matches: " + failureCount);
				}
				synchronized (LockObj) {
					LockObj.notify();
				}
			}
		}

		@Override
		public void nextTuple() {
			String gender;
		
			if ((streamsToProcess > 0) && (submittedCount == streamsToProcess)) {
				Utils.sleep(1);
				return;
			}
		
			if (submittedCount % 2 == 0) {
				gender = "male";
			} else {
				gender = "female";
			}
			feed(new Values(keyValue, gender, ranGen.nextInt(10000)),
					new Integer(submittedCount));
			super.nextTuple();
			submittedCount++;
			if (++keyValue >= highestKeyValue) {
				keyValue = 0;
			}
		}
	}
}

