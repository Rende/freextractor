/**
 *
 */
package de.dfki.mlt.diretc.flink.term;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import de.dfki.mlt.diretc.App;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class TermCountingMap implements FlatMapFunction<String, Tuple4<String, Double, Double, String>> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Override
	public void flatMap(String clusterId, Collector<Tuple4<String, Double, Double, String>> out) throws Exception {
		HashMap<String, Double> dict = new HashMap<String, Double>();
		SearchResponse response = App.esService.getClusterEntryHits(clusterId);
		do {
			for (SearchHit hit : response.getHits().getHits()) {
				List<Map<Object, Object>> wordMap = (List<Map<Object, Object>>) hit.getSourceAsMap().get("words");
				for (Map<Object, Object> entry : wordMap) {
					String word = (String) entry.get("word");
					Integer count = (Integer) entry.get("count");
					Double dblCount = Double.parseDouble(count.toString());
					if (dict.containsKey(word)) {
						dblCount += dict.get(word);
					}
					dict.put(String.valueOf(word), dblCount);
				}
			}
			response = App.esService.getClient().prepareSearchScroll(response.getScrollId())
					.setScroll(new TimeValue(60000)).execute().actionGet();
		} while (response.getHits().getHits().length != 0);

		double total = 0;
		for (Entry<String, Double> entry : dict.entrySet()) {
			total += entry.getValue();
		}
		for (Entry<String, Double> entry : dict.entrySet()) {
			double tf = entry.getValue() / total;
			out.collect(new Tuple4<String, Double, Double, String>(entry.getKey(), tf, 0.0, clusterId));
		}

	}

}
