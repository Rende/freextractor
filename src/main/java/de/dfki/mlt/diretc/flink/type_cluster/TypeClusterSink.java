/**
 *
 */
package de.dfki.mlt.diretc.flink.type_cluster;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import de.dfki.mlt.diretc.App;
import de.dfki.mlt.diretc.preferences.Config;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class TypeClusterSink implements ElasticsearchSinkFunction<TypeClusterMember> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public IndexRequest createIndexRequest(TypeClusterMember typeClusterMember) throws IOException {

		XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
				.field("subj-type", typeClusterMember.getClusterId().getSubjectType())
				.field("obj-type", typeClusterMember.getClusterId().getObjectType())
				.field("relation", typeClusterMember.getClusterId().getRelationLabel())
				.field("is-cluster-member", typeClusterMember.getIsClusterMember())
				.field("relation-id", typeClusterMember.getClusterId().getRelationId())
				.field("cluster-id", typeClusterMember.getClusterId().toString())
				.field("subj-name", typeClusterMember.getSubjectName())
				.field("subj-id", typeClusterMember.getSubjectId())
				.field("obj-name", typeClusterMember.getObjectName())
				.field("obj-id", typeClusterMember.getObjectId())
				.field("relation-phrase", typeClusterMember.getRelationPhrase())
				.field("sent", typeClusterMember.getSentence())
				.field("lem-sent", typeClusterMember.getTokenizedSentence())
				.field("page-id", typeClusterMember.getPageId())
				.field("subj-pos", typeClusterMember.getSubjectPosition())
				.field("obj-pos", typeClusterMember.getObjectPosition())
				.field("relation-phrase-bow", typeClusterMember.getBagOfWords()).startArray("words");

		for (Map.Entry<String, Integer> entry : typeClusterMember.getHistogram().entrySet()) {
			builder.startObject().field("word", entry.getKey()).field("count", entry.getValue()).endObject();
		}
		builder.endArray().endObject();
		IndexRequest indexRequest = Requests.indexRequest()
				.index(Config.getInstance().getString(Config.TYPE_CLUSTER_INDEX))
				.type(Config.getInstance().getString(Config.CLUSTER_MEMBER)).source(builder);

		return indexRequest;
	}

	@Override
	public void process(TypeClusterMember entry, RuntimeContext ctx, RequestIndexer indexer) {
		try {
			indexer.add(createIndexRequest(entry));
		} catch (IOException e) {
			App.LOG.error("ClusterSinkFunction: " + e, e);
		}
	}

}
