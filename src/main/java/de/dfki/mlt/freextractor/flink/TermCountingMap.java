/**
 *
 */
package de.dfki.mlt.freextractor.flink;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.search.SearchRequestBuilder;
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
public class TermCountingMap implements
		FlatMapFunction<String, Tuple4<String, Double, Double, String>> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("deprecation")
	@Override
	public void flatMap(String clusterId,
			Collector<Tuple4<String, Double, Double, String>> out)
			throws Exception {
		int scrollSize = 10000;
		HashMap<String, Double> dict = new HashMap<String, Double>();
		SearchRequestBuilder builder = App.esService
				.getClient()
				.prepareSearch(
						Config.getInstance().getString(
								Config.CLUSTER_ENTRY_INDEX))
				.setSearchType(SearchType.SCAN).setScroll(new TimeValue(60000))
				.setTypes(Config.getInstance().getString(Config.CLUSTER_ENTRY))
				.addFields("words.word", "words.count")
				.setQuery(QueryBuilders.matchQuery("cluster-id", clusterId));
		System.out.println(builder.toString());
		SearchResponse response = builder.setSize(scrollSize).execute()
				.actionGet();
		double count = 0;
		do {
			for (SearchHit hit : response.getHits().getHits()) {
				if (hit.field("words.word") != null) {
					List<Object> words = hit.field("words.word").getValues();
					List<Object> counts = hit.field("words.count").getValues();
					for (int i = 0; i < words.size(); i++) {
						count = Integer.parseInt(counts.get(i).toString());
						if (dict.containsKey(words.get(i))) {
							count += dict.get(words.get(i));
						}
						dict.put(String.valueOf(words.get(i)), count);
					}
				}
			}
			response = App.esService.getClient()
					.prepareSearchScroll(response.getScrollId())
					.setScroll(new TimeValue(60000)).execute().actionGet();
		} while (response.getHits().getHits().length != 0);

		double total = 0;
		for (Entry<String, Double> entry : dict.entrySet()) {
			total += entry.getValue();
		}
		for (Entry<String, Double> entry : dict.entrySet()) {
			double tf = entry.getValue() / total;
			System.out.println("Tf: " + tf);
			out.collect(new Tuple4<String, Double, Double, String>(entry
					.getKey(), tf, 0.0, clusterId));
		}

	}

}
