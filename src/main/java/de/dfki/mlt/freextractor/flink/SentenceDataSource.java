/**
 *
 */
package de.dfki.mlt.freextractor.flink;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import de.dfki.mlt.freextractor.App;
import de.dfki.mlt.freextractor.preferences.Config;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class SentenceDataSource implements
		SourceFunction<Tuple5<Integer, String, String, String, String>> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	boolean isRunning = true;

	@SuppressWarnings("deprecation")
	@Override
	public void run(SourceContext<Tuple5<Integer, String, String, String, String>> ctx)
			throws Exception {
		int scrollSize = 1000;
		SearchResponse response = App.esService
				.getClient()
				.prepareSearch(
						Config.getInstance().getString(
								Config.WIKIPEDIA_SENTENCE_INDEX))
				.setSearchType(SearchType.SCAN)
				.setScroll(new TimeValue(60000))
				.setTypes(
						Config.getInstance().getString(
								Config.WIKIPEDIA_SENTENCE))
				.addFields("page-id", "title", "subject-id", "sentence", "tok-sentence")
				.setQuery(QueryBuilders.matchAllQuery()).setSize(scrollSize)
				.execute().actionGet();
		do {
			for (SearchHit hit : response.getHits().getHits()) {
				Integer pageId = Integer.parseInt(hit.field("page-id")
						.getValue().toString());
				String subjectId = hit.field("subject-id").getValue()
						.toString();
				String title = hit.field("title").getValue().toString();
				String sentence = hit.field("sentence").getValue().toString();
				String tokenizedSentence = hit.field("tok-sentence").getValue().toString();
				ctx.collect(new Tuple5<Integer, String, String, String,String>(pageId,
						subjectId, title, sentence,tokenizedSentence));
			}
			response = App.esService.getClient()
					.prepareSearchScroll(response.getScrollId())
					.setScroll(new TimeValue(60000)).execute().actionGet();
		} while (isRunning && response.getHits().getHits().length != 0);

	}

	@Override
	public void cancel() {
		isRunning = false;
	}

}
