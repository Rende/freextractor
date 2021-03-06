/**
 *
 */
package de.dfki.mlt.diretc.flink.type_cluster;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import de.dfki.mlt.diretc.App;
import de.dfki.mlt.diretc.preferences.Config;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class SentenceDataSource implements SourceFunction<Tuple5<Integer, List<String>, String, String, String>> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	boolean isRunning = true;

	@SuppressWarnings("unchecked")
	@Override
	public void run(SourceContext<Tuple5<Integer, List<String>, String, String, String>> ctx) throws Exception {
		SearchRequestBuilder request = App.esService.getClient()
				.prepareSearch(Config.getInstance().getString(Config.WIKIPEDIA_SENTENCE_INDEX))
				.setScroll(new TimeValue(300000)).setTypes(Config.getInstance().getString(Config.WIKIPEDIA_SENTENCE))
				.setQuery(QueryBuilders.matchAllQuery()).setSize(Config.getInstance().getInt(Config.SCROLL_SIZE));
		SearchResponse response = request.execute().actionGet();
		do {
			for (SearchHit hit : response.getHits().getHits()) {
				Integer pageId = Integer.parseInt(hit.getSourceAsMap().get("page-id").toString());
				List<String> candidateSubjs = (List<String>) hit.getSourceAsMap().get("subject-id");
				String title = hit.getSourceAsMap().get("title").toString();
				String sentence = hit.getSourceAsMap().get("sentence").toString();
				String lemSentence = hit.getSourceAsMap().get("lem-sentence").toString();
				if (!(candidateSubjs.isEmpty() || candidateSubjs.get(0) == ""))
					ctx.collect(new Tuple5<Integer, List<String>, String, String, String>(pageId, candidateSubjs, title,
							sentence, lemSentence));
			}
			response = App.esService.getClient().prepareSearchScroll(response.getScrollId())
					.setScroll(new TimeValue(300000)).execute().actionGet();
		} while (isRunning && response.getHits().getHits().length != 0);

	}

	@Override
	public void cancel() {
		isRunning = false;
	}

}
