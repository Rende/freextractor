package de.dfki.mlt.freextractor;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.dfki.mlt.freextractor.flink.SentenceDatasource;
import de.dfki.mlt.freextractor.flink.cluster_entry.ClusterEntryMap;
import de.dfki.mlt.freextractor.flink.cluster_entry.ClusterSink;

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

		env.setParallelism(20);

		stream.flatMap(new ClusterEntryMap()).addSink(
				new ElasticsearchSink<>(ElasticsearchService.getUserConfig(),
						ElasticsearchService.getTransportAddresses(),
						new ClusterSink()));

		// DataStream<String> stream = env.addSource(new ClusterIdDataSource());
		// env.setParallelism(20);
		// try {
		// esService.checkAndCreateIndex(Config.getInstance().getString(
		// Config.TERM_INDEX));
		// esService.putMappingForTerms();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		//
		// stream.flatMap(new TermCountingMap())
		// .keyBy(0)
		// .addSink(
		// new ElasticsearchSink<>(ElasticsearchService
		// .getUserConfig(), ElasticsearchService
		// .getTransportAddresses(), new TermSink()));

		// .countWindow(2000)
		// .apply(new WindowFunction<Tuple4<String, Integer, Integer, String>,
		// Tuple4<String, Integer, Integer, String>, Tuple, GlobalWindow>() {
		// @Override
		// public void apply(
		// Tuple key,
		// GlobalWindow window,
		// Iterable<Tuple4<String, Integer, Integer, String>> input,
		// Collector<Tuple4<String, Integer, Integer, String>> out)
		// throws Exception {
		// int sum = 0;
		// for (Tuple4<String, Integer, Integer, String> t : input) {
		// sum += t.f2;
		// }
		// for (Tuple4<String, Integer, Integer, String> t : input) {
		// t.f2 = sum;
		// out.collect(t);
		// }
		// }
		// })
		// // .sum(2)
		// .addSink(
		// new ElasticsearchSink<>(ElasticsearchService
		// .getUserConfig(), ElasticsearchService
		// .getTransportAddresses(), new TermSink()));

		env.execute("Freextractor");

	}
}
