package de.dfki.mlt.freextractor;

import java.io.IOException;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.dfki.mlt.freextractor.flink.RelationExtractionMap;
import de.dfki.mlt.freextractor.flink.RelationSink;
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

		try {
			esService.checkAndCreateIndex(Config.getInstance().getString(
					Config.WIKIPEDIA_RELATION_INDEX));

		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment().setParallelism(10);

		DataStream<Tuple4<Integer, String, String, String>> stream = env
				.addSource(new SentenceDatasource());
		// stream.map(
		// new MapFunction<Tuple4<Integer, String, String, String>, String>() {
		//
		// @Override
		// public String map(
		// Tuple4<Integer, String, String, String> value)
		// throws Exception {
		// return value.f3.trim();
		// }
		// }).writeAsText("/Users/aydanrende/Documents/sentences",
		// WriteMode.NO_OVERWRITE);
		stream.flatMap(new RelationExtractionMap()).addSink(
				new ElasticsearchSink<>(ElasticsearchService.getUserConfig(),
						ElasticsearchService.getTransportAddresses(),
						new RelationSink()));

		env.execute("Freextractor");

	}
}
