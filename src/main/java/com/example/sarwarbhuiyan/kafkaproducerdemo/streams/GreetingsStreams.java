package com.example.sarwarbhuiyan.kafkaproducerdemo.streams;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "kafka.streams.greetings-streams")
public class GreetingsStreams {

	private static final String GREETINGS_COUNTS = "greetings-counts";

	private static final String COUNTS_KEY_VALUE_STORE = "CountsKeyValueStore";

	private static Logger logger = LoggerFactory.getLogger(GreetingsStreams.class);

	@Value("${kafka.streams.greetings-streams.input-topic}")
	private String greetingsTopic;

	private KafkaStreams streams;

	private Properties properties;

	private ReadOnlyKeyValueStore<String, Long> keyValueStore;

	public void setProperties(Properties properties) {
		this.properties = properties;
		this.properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_beta");
		
		// Don't do this
		// properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		
	}

	public Topology buildTopology() {
		StreamsBuilder builder = new StreamsBuilder();
		//Always use Consumed.with() when you call builder.stream or builder.table or builder.globalKTable
		KStream<String, String> greetings = builder.stream(greetingsTopic, 
															Consumed.with(Serdes.String(), 
																	Serdes.String()));
		
		KTable<String, Long> greetingsCount = greetings
				.groupByKey()
				.count(Materialized.as(COUNTS_KEY_VALUE_STORE));

		greetingsCount
			.toStream()
			
			//Always use Produced.with when doing to
			.to(GREETINGS_COUNTS, 
					Produced.with(Serdes.String(), 
							Serdes.Long()));

		return builder.build();
	}

	/**
	 * A dummy example of querying a state story in an interactive query
	 * This example does not show what's required when you have multiple
	 * instances of the Kafka Streams app and you need to make cross-instance
	 * calls via some sort of RPC
	 * 
	 * @return
	 */
	public Map<String, String> getLatestCounts() {
		Map<String, String> results = new HashMap<>();
		KeyValueIterator<String, Long> iter = keyValueStore.all();
		while (iter.hasNext()) {
			KeyValue<String, Long> kv = iter.next();
			results.put(new String(kv.key), kv.value.toString());
		}
		logger.info("KTable Counts = " + results);
		return results;
	}

	/**
	 * This starts the streams application 
	 */
	@PostConstruct
	public void start() {
		logger.info("Starting Greetings Streams App");
		streams = new KafkaStreams(buildTopology(), properties);
		streams.cleanUp();
		
		// This ensures any uncaught exceptions would allow the stream thread to be recreated. 
		streams.setUncaughtExceptionHandler(ex -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD);
		streams.start();
	
		keyValueStore = streams.store(
				StoreQueryParameters.fromNameAndType(COUNTS_KEY_VALUE_STORE, QueryableStoreTypes.keyValueStore()));

	}

	@PreDestroy
	public void stop() {
		logger.info("Stopping Greetings Streams App and cleaning up");
		if (streams != null)
			streams.close(Duration.ofMinutes(5));

	}

}
