package de.dfki.mlt.freextractor;

import java.io.IOException;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.dfki.mlt.freextractor.flink.ClusterSink;
import de.dfki.mlt.freextractor.flink.ClusteringMap;
import de.dfki.mlt.freextractor.flink.SentenceDatasource;
import de.dfki.mlt.freextractor.preferences.Config;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class App {
	public static final Logger LOG = LoggerFactory.getLogger(App.class);
	public static ElasticsearchService esService = new ElasticsearchService();

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		DataStream<Tuple5<Integer, String, String, String, String>> stream = env
				.addSource(new SentenceDatasource());
		// if (Config.getInstance().getBoolean(Config.RELEX_MODE)) {
		// env.setParallelism(10);
		// try {
		// // esService.checkAndCreateIndex(Config.getInstance().getString(
		// // Config.WIKIPEDIA_INDEX));
		// esService.putMappingForClusterEntry();
		// esService.putMappingForRelations();
		//
		// } catch (IOException | InterruptedException e) {
		// e.printStackTrace();
		// }
		//
		// stream.flatMap(new RelationExtractionMap()).addSink(
		// new ElasticsearchSink<>(ElasticsearchService
		// .getUserConfig(), ElasticsearchService
		// .getTransportAddresses(), new RelationSink()));
		// }
		if (Config.getInstance().getBoolean(Config.CLUSTER_MODE)) {
			env.setParallelism(10);
			try {
				esService.checkAndCreateIndex(Config.getInstance().getString(
						Config.CLUSTER_ENTRY_INDEX));
				esService.putMappingForClusterEntry();
			} catch (IOException e) {
				e.printStackTrace();
			}

			stream.flatMap(new ClusteringMap()).addSink(
					new ElasticsearchSink<>(ElasticsearchService
							.getUserConfig(), ElasticsearchService
							.getTransportAddresses(), new ClusterSink()));
		}

		env.execute("Freextractor");

	}
}
