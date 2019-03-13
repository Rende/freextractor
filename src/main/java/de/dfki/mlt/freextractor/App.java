package de.dfki.mlt.freextractor;

import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.dfki.mlt.freextractor.flink.Helper;
import de.dfki.mlt.freextractor.flink.cluster_entry.ClusterEntry;
import de.dfki.mlt.freextractor.flink.cluster_entry.ClusterEntryMap;
import de.dfki.mlt.freextractor.flink.cluster_entry.ClusterEntrySink;
import de.dfki.mlt.freextractor.flink.cluster_entry.SentenceDataSource;
import de.dfki.mlt.freextractor.flink.term.ClusterIdDataSource;
import de.dfki.mlt.freextractor.flink.term.DocCountingMap;
import de.dfki.mlt.freextractor.flink.term.TermCountingMap;
import de.dfki.mlt.freextractor.flink.term.TermDataSource;
import de.dfki.mlt.freextractor.flink.term.TermSink;
import de.dfki.mlt.freextractor.flink.term.TfIdfSink;
import de.dfki.mlt.freextractor.preferences.Config;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class App {
	public static final Logger LOG = LoggerFactory.getLogger(App.class);

	public static ElasticsearchService esService = new ElasticsearchService();
	public static Helper helper = new Helper();

	public static void main(String[] args) throws Exception {

		sentenceProcessingApp();
		// termCountingApp();
		// docCountingApp();

	}

	/**
	 * App1: Gets all sentences, creates sentence stream, maps to the nodes. Each
	 * node processes the sentences and creates a cluster entry per sentence which
	 * contains: cluster_id = subject_type + object_type + relation_label, histogram
	 * = <word, count> and the positions of the subject and object in the sentence.
	 * Each cluster entry is sunk into ES at the end.
	 */
	public static boolean sentenceProcessingApp() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(10);
		esService.checkAndCreateIndex(Config.getInstance().getString(Config.CLUSTER_ENTRY_INDEX));
		esService.putMappingForClusterEntry();
		DataStream<Tuple5<Integer, List<String>, String, String, String>> stream = env.addSource(new SentenceDataSource());
		stream.flatMap(new ClusterEntryMap())
				.addSink(new ElasticsearchSink<ClusterEntry>(ElasticsearchService.getUserConfig(),
						ElasticsearchService.getTransportAddresses(), new ClusterEntrySink()));
		JobExecutionResult result = env.execute("SentenceProcessingApp");
		return result.isJobExecutionResult();
	}

	/**
	 * App2: Gets all cluster ids, creates id stream, maps to the nodes. Each node
	 * retrieves all cluster entries that belong to the cluster id. Creates a
	 * dictionary for one cluster which contains terms and tf (normalized tf). Then
	 * these dictionaries are sunk term by term into ES.
	 */
	public static boolean termCountingApp() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(14);
		DataStream<String> stream = env.addSource(new ClusterIdDataSource());
		esService.checkAndCreateIndex(Config.getInstance().getString(Config.TERM_INDEX));
		esService.putMappingForTerms();
		stream.flatMap(new TermCountingMap()).addSink(new ElasticsearchSink<>(ElasticsearchService.getUserConfig(),
				ElasticsearchService.getTransportAddresses(), new TermSink()));
		JobExecutionResult result = env.execute("termCountingApp");
		return result.isJobExecutionResult();
	}

	/**
	 * App3: Gets all terms and computes idf for all terms, creates <term, idf>
	 * stream. Each node retrieves the term occurrences in different clusters,
	 * computes tf*idf. Then the terms are updated with the tf*idf scores in ES.
	 */
	public static boolean docCountingApp() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(14);
		env.addSource(new TermDataSource()).flatMap(new DocCountingMap()).addSink(new ElasticsearchSink<>(
				ElasticsearchService.getUserConfig(), ElasticsearchService.getTransportAddresses(), new TfIdfSink()));
		JobExecutionResult result = env.execute("docCountingApp");
		return result.isJobExecutionResult();
	}

}
