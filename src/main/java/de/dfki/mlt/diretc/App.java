package de.dfki.mlt.diretc;

import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.dfki.mlt.diretc.flink.term.ClusterIdDataSource;
import de.dfki.mlt.diretc.flink.term.DocCountingMap;
import de.dfki.mlt.diretc.flink.term.TermCountingMap;
import de.dfki.mlt.diretc.flink.term.TermDataSource;
import de.dfki.mlt.diretc.flink.term.TermSink;
import de.dfki.mlt.diretc.flink.term.TfIdfSink;
import de.dfki.mlt.diretc.flink.type_cluster.SentenceDataSource;
import de.dfki.mlt.diretc.flink.type_cluster.TypeClusterMap;
import de.dfki.mlt.diretc.flink.type_cluster.TypeClusterMember;
import de.dfki.mlt.diretc.flink.type_cluster.TypeClusterSink;
import de.dfki.mlt.diretc.preferences.Config;

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
		esService.stopConnection();

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
		esService.checkAndCreateIndex(Config.getInstance().getString(Config.TYPE_CLUSTER_INDEX));
		esService.putMappingForClusterEntry();
		DataStream<Tuple5<Integer, List<String>, String, String, String>> stream = env
				.addSource(new SentenceDataSource());
		SingleOutputStreamOperator<TypeClusterMember> typeClusterStream = stream.flatMap(new TypeClusterMap());
		ElasticsearchSink.Builder<TypeClusterMember> esSinkBuilder = new ElasticsearchSink.Builder<>(
				esService.getHosts(), new TypeClusterSink());
		esSinkBuilder.setBulkFlushMaxActions(Config.getInstance().getInt(Config.BULK_FLUSH_MAX_ACTIONS));
		typeClusterStream.addSink(esSinkBuilder.build());

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
		SingleOutputStreamOperator<Tuple4<String, Double, Double, String>> termStream = stream
				.flatMap(new TermCountingMap());
		ElasticsearchSink.Builder<Tuple4<String, Double, Double, String>> esSinkBuilder = new ElasticsearchSink.Builder<>(
				esService.getHosts(), new TermSink());
		esSinkBuilder.setBulkFlushMaxActions(Config.getInstance().getInt(Config.BULK_FLUSH_MAX_ACTIONS));
		termStream.addSink(esSinkBuilder.build());
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
		SingleOutputStreamOperator<Tuple2<String, Double>> docStream = env.addSource(new TermDataSource())
				.flatMap(new DocCountingMap());
		ElasticsearchSink.Builder<Tuple2<String, Double>> esSinkBuilder = new ElasticsearchSink.Builder<>(
				esService.getHosts(), new TfIdfSink());
		esSinkBuilder.setBulkFlushMaxActions(Config.getInstance().getInt(Config.BULK_FLUSH_MAX_ACTIONS));
		docStream.addSink(esSinkBuilder.build());
		JobExecutionResult result = env.execute("docCountingApp");
		return result.isJobExecutionResult();
	}

}
