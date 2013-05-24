Storm-Aerospike Project
=======================
Contents
--------
This project consists of 3 Storm Bolts and a test client. An assumption is made that you understand the concepts regarding Storm and the Aerospike NoSQL database.

To use this package, you will need to have Storm as well as Aerospike Java API installed. The server(s) running Storm will require access to an Aerospike database. Aerospike offers a free Community Edition that will allow you to store up to 200GB of data in an Aerospike database cluster composed of up to 2 server nodes. The Aerospike Monitoring Console provides an excellent means of displaying the state of the Aerospike cluster and graphically representing the current read and write throughput in a browser window. It is available for free download.

The Aerospike NoSQL Database
----------------------------
Aerospike is capable of executing well over *1 Million transactions per second* at very low latency, while operating **24x7x365** on a cluster of low-cost commodity servers, with data either in DRAM, or in SSDs.
For detail on how to install/use Aerospike, please refer to the [Aerospike documentation](https://docs.aerospike.com)

Bolts
-----
Two Bolts are intended for reuse in other projects. AerospikePersistBolt will persist Tuples in an incoming stream to an Aerospike NoSQL database. AerospikeEnrichBolt will use incoming Tuple key values to retrieve records from an Aerospike database and then emit those records as Java Map objects.

The name of the key value, for each of those bolts is "key". The instantiation method for each of those bolts may optionally override the name of the key value. Those instantiation methods are required to provide the Aerospike host, port, namespace, and set which you desire the Bolts to access.

The dependencies, as are visible from the CLASSPATH below are on the Storm 0.8.2 package, the Apache Commons CLI package, the Aerospike 2.1.16 client package, and the gnu-crypto package (for Aerospike). It has not been tested with any other packages. The PATHs below will have to be modified for the appropriate locations in your environment if you are utilizing Storm in a local mode of operation.

Build the package with the "mvn package" command. To package a jar with all dependencies, excluding storm, for deployment to distributed mode use the "mvn assembly:assembly" command.

Test/Demonstration
------------------
The other Bolt (AerospikeValidationBolt) and the Test program/Spout (AerospikeRoundTripTest) are very useful for demonstration/testing purposes. Most of the remainder of this README covers the design of the testing Topology and it's options.

The test program options are displayed as follows:

java storm.contrib.aerospike.test.AerospikeRoundTripTest -help    
usage: java AerospikeRoundTripTest    
     -dbg          turn on Topology debugging (default false)    
     -et <arg>     number of enrich Bolt threads (default 1)    
     -h <arg>      Aerospike host (default 127.0.0.1)    
     -help         help    
     -l <arg>      highest key value before looping back to 0. 0 = don't loop (default 0)    
     -n <arg>      Aerospike namespace (default test)    
     -p <arg>      Aerospike port (default 3000)    
     -pt <arg>     number of persist Bolt threads (default 1)    
     -r <arg>      number of records(streams) to submit. 0 = infinite (default 1)    
     -s <arg>      Aerospike set (default Bolt)    
     -seed <arg>   Random field seed value (default 1)    
     -st           submit Topology to a Storm cluster (default false)    
     -v            validate the insertions by reading and comparing (default false)    
     -vo           validate by reading and comparing to expected values (default false)    
     -vt <arg>     number of validate Bolt threads (default 1)    
     -w <arg>      number of storm workers (default 4)    

The default operation of the test program is to set up a local mode Storm Topology that will encompass a test Spout along with a single Bolt. The Bolt will connect to an Aerospike cluster using the default, host, port, and namespace. The Spout will emit a single tuple consisting of 3 name/value pairs onto the stream. The Bolt will receive the tuple and persist the tuple in the Aerospike database on the default set. The key will be the numeric value '1'. The following displays the default Storm Topology.

`AerospikeRoundTripTest -> AerospikePersistBolt -> Aerospike DB`

A few other Topologies are available in the package. The use of the `-v` option will cause a the Topology to be expanded with the addition of 2 Bolts, AerospikeEnrichBolt, and AerospikeValidationBolt. Each tuple persisted into the Aerospike database by AerspikePersistanceBolt will then be read back from the Aerospike database by AerospikeEnrichBolt and compared against the data that was written to the DB by AerospikeValidationBolt.

`AerospikeRoundTripTest -> AerospikePersistBolt -> AerospikeEnrichBolt -> AerospikeValidationBolt`

`AerospikeRoundTripTest ->AerospikeValidationBolt`
  

The use of the "-vo" option will only read the data previously written to the Aerospike database, for comparison against expected values.

`AerospikeRoundTripTest -> AerospikeEnrichBoltBolt -> AerospikeValidationBolt` 

`AerospikeRoundTripTest -> AerospikeValidationBolt`               
                 
If you wish to execute the desired Topology in Storm distributed mode, you will need to use the `-st` option, as Storm local mode is the default.

The number of tuples to be persisted can be increased via the `-r` option. The key values for the records created will start at 1 and increment until the requested number of tuples are processed. Using the `-r 0` option will cause the Spout to emit Streams forever. When using the `-r 0` option, it is highly recommended that you also use the `-l` option which will cause the key value to loop back to zero when reaching a defined limit. So, using `-r 0 -l 1000000` will cause the spout to emit records with key values 1 through 1000000 and then look back to key value 1 again continuously until the spout is killed.

One of the 3 field emitted by the test program is a random numeric value. The seed for that value may be changed throat the use of the `-seed` option.

The number of persistence Bolts thread instances may by increased via the `-pt` option. Additional threads for persistence and enrichment will provide greater throughput for Aerospike database access. Additional threads for the validation bolt will help eliminate that bolt as a bottleneck. Be aware that when executing the test, your throughput may be limited by not having sufficient threads and/or sufficient servers executing the Storm Topology. In our testing a 3-node Aerospike cluster was fairly idle, when being driven by a single client server executing the Topology which was at approximately 100% CPU utilization.

At the end of the test, if executing in local mode, the number of tuples processed, along with comparison error count (if any) are displayed on stdout.
Test timings and throughput results are also displayed.

The remainder of the options are self-explanatory.

Representative environment variable settings
------------
export AS_JAVA_HOME="/home/rek/Downloads/aerospike-client-java-2.1.16"

export CLASSPATH=".:/home/rek/Downloads/storm-contrib-master/storm-aerospike/target/classes:/home/rek/Downloads/storm-0.8.2/lib/*:/home/rek/Downloads/storm-0.8.2/storm-0.8.2.jar:/home/rek/Downloads/aerospike-client-java-2.1.16/client/depends/gnu-crypto.jar:/home/rek/Downloads/commons-cli-1.2/commons-cli-1.2.jar"

Test program examples
---------------------
`storm jar storm-contrib-aerospike-0.1-SNAPSHOT-jar-with-dependencies.jar AerospikeRoundTripTest -h 192.168.25.64 -r 0 \`     
`-l 1000000 -st -pt 4 -et 8 -vt 8 -w 4 -v`

Execute the Topology in distributed mode by providing the jar to storm, submitting the Topology including validation of persisted data (-v) for execution. Execute the Spout forever (-r 0), looping at record # 1000000 (-l 1000000), with one of the Aerospike server nodes being at IP address 192.168.25.64, utilizing 4 AerospikePersistanceBolt threads, 8 AerospikeEnrichBolt threads, and 8 AerospikeValidationBolt threads spread among 4 processes (-w 4)

`java AerospikeRoundTripTest -r 1000000 -pt 4 -w 2`

Execute the Topology in local mode, simply persisting 1000000 records to Aerospike using 4 AerospikePersistanceBolt threads executing within 2 Storm worker processes, with the default Aerospike connection settings.

Support
-------
For questions/support, please post on the [Aerospike Storm Interface Forum](https://forums.aerospike.com/viewforum.php?f=27)

