/**
 *
 */
package de.dfki.mlt.freextractor.flink.term;

import java.io.IOException;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import de.dfki.mlt.freextractor.App;
import de.dfki.mlt.freextractor.preferences.Config;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class TermSink implements
		ElasticsearchSinkFunction<Tuple4<String, Double, Double, String>> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public IndexRequest createIndexRequest(
			Tuple4<String, Double, Double, String> element) throws IOException {

		XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
				.field("term", element.f0).field("tf", element.f1)
				.field("tf-idf", element.f2).field("cluster-id", element.f3)
				.endObject();

		IndexRequest indexRequest = Requests.indexRequest()
				.index(Config.getInstance().getString(Config.TERM_INDEX))
				.type(Config.getInstance().getString(Config.TERM)).source(builder);

		return indexRequest;
	}

	@Override
	public void process(Tuple4<String, Double, Double, String> element,
			RuntimeContext ctx, RequestIndexer indexer) {
		try {
			indexer.add(createIndexRequest(element));
		} catch (IOException e) {
			App.LOG.error("TermSinkFunction: " + e, e);
		}
	}

}
