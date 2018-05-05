/**
 *
 */
package de.dfki.mlt.freextractor.flink.cluster_entry;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
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
public class ClusterEntrySink implements ElasticsearchSinkFunction<ClusterEntry> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public IndexRequest createIndexRequest(ClusterEntry clusterEntry) throws IOException {

		XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
				.field("subj-type", clusterEntry.getClusterId().getSubjectType())
				.field("obj-type", clusterEntry.getClusterId().getObjectType())
				.field("relation", clusterEntry.getClusterId().getRelationLabel())
				.field("relation-id", clusterEntry.getClusterId().getRelationId())
				.field("cluster-id", clusterEntry.getClusterId().toString())
				.field("subj-name", clusterEntry.getSubjectName())
				.field("obj-name", clusterEntry.getObjectName())
				.field("relation-phrase", clusterEntry.getRelationPhrase())
				.field("tok-sent", clusterEntry.getTokenizedSentence())
				.field("page-id", clusterEntry.getPageId())
				.field("subj-pos", clusterEntry.getSubjectPosition())
				.field("obj-pos", clusterEntry.getObjectPosition())
				.startArray("words");

		for (Map.Entry<String, Integer> entry : clusterEntry.getHistogram().entrySet()) {
			builder.startObject().field("word", entry.getKey()).field("count", entry.getValue()).endObject();
		}
		builder.endArray().endObject();
		String json = builder.string();
		IndexRequest indexRequest = Requests.indexRequest()
				.index(Config.getInstance().getString(Config.CLUSTER_ENTRY_INDEX))
				.type(Config.getInstance().getString(Config.CLUSTER_ENTRY)).source(json);

		return indexRequest;
	}

	@Override
	public void process(ClusterEntry entry, RuntimeContext ctx, RequestIndexer indexer) {
		try {
			indexer.add(createIndexRequest(entry));
		} catch (IOException e) {
			App.LOG.error("ClusterSinkFunction: " + e, e);
		}
	}

}
