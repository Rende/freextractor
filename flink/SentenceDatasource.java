/**
 *
 */
package de.dfki.mlt.wre.flink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import de.dfki.mlt.wre.WikiRelationExtractionApp;
import de.dfki.mlt.wre.preferences.Config;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class SentenceDatasource implements
		SourceFunction<Tuple3<String, String, String>> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	boolean isRunning = true;

	@Override
	public void run(SourceContext<Tuple3<String, String, String>> ctx)
			throws Exception {
		int scrollSize = 1000;
		SearchResponse response = null;
		int i = 0;
		while (response == null || response.getHits().hits().length != 0
				&& isRunning) {
			response = WikiRelationExtractionApp.esService
					.getClient()
					.prepareSearch(
							Config.getInstance().getString(
									Config.WIKIPEDIA_INDEX))
					.setTypes(
							Config.getInstance().getString(
									Config.WIKIPEDIA_SENTENCE))
					.addFields("title", "sentence")
					.setQuery(QueryBuilders.matchAllQuery())
					.setSize(scrollSize).setFrom(i * scrollSize).execute()
					.actionGet();
			for (SearchHit hit : response.getHits().getHits()) {
				String id = hit.getId();
				String title = hit.field("title").getValue().toString();
				String sentence = hit.field("sentence").getValue().toString();
				ctx.collect(new Tuple3<String, String, String>(id, title,
						sentence));
			}
			i++;
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

}
