/**
 *
 */
package de.dfki.mlt.freextractor.flink.relation_extraction;

import java.io.IOException;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
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
public class RelationSink implements ElasticsearchSinkFunction<Relation> {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	// filename, url, title, index, sentence, lang, tokenlist
	public IndexRequest createIndexRequest(Relation relation)
			throws IOException {
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
				.field("page-id", relation.getPageId())
				.field("subject-id", relation.getSubjectId())
				.field("subject-index", relation.getSubjectIndex())
				.field("object-id", relation.getObjectId())
				.field("object-index", relation.getObjectIndex())
				.field("surface", relation.getSurface())
				.field("start-index", relation.getStartIndex())
				.field("end-index", relation.getEndIndex())
				.field("property-id", relation.getPropId())
				.field("alias", relation.getAlias()).endObject();
		String json = builder.string();
		IndexRequest indexRequest = Requests
				.indexRequest()
				.index(Config.getInstance().getString(
						Config.WIKIPEDIA_RELATION_INDEX))
				.type(Config.getInstance().getString(Config.WIKIPEDIA_RELATION))
				.source(json);

		return indexRequest;
	}

	@Override
	public void process(Relation relation, RuntimeContext ctx,
			RequestIndexer indexer) {
		try {
			indexer.add(createIndexRequest(relation));
		} catch (Exception exception) {
			App.LOG.error("RelationSinkFunction: " + exception, exception);
		}
	}
}
