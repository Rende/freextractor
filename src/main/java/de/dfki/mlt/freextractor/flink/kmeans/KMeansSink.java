package de.dfki.mlt.freextractor.flink.kmeans;

import java.io.IOException;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import de.dfki.mlt.freextractor.App;
import de.dfki.mlt.freextractor.preferences.Config;

public class KMeansSink implements ElasticsearchSinkFunction<Tuple2<String, String>> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public UpdateRequest createIndexRequest(Tuple2<String, String> element) throws IOException {

		XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("is-cluster-member", true).endObject();
		String json = builder.string();

		UpdateRequest request = new UpdateRequest();
		request.index(Config.getInstance().getString(Config.CLUSTER_ENTRY_INDEX))
				.type(Config.getInstance().getString(Config.CLUSTER_ENTRY)).id(element.f1).doc(json);

		return request;
	}

	@Override
	public void process(Tuple2<String, String> element, RuntimeContext ctx, RequestIndexer indexer) {
		try {
			indexer.add(createIndexRequest(element));
		} catch (IOException e) {
			App.LOG.error("KmeansSink: " + e, e);
		}

	}

}
