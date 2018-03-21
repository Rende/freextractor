/**
 *
 */
package de.dfki.mlt.freextractor.flink.term;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import de.dfki.mlt.freextractor.App;
import de.dfki.mlt.freextractor.preferences.Config;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class DocCountingMap implements
		FlatMapFunction<Tuple2<Double, String>, Tuple2<String, Double>> {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(Tuple2<Double, String> value,
			Collector<Tuple2<String, Double>> out) throws Exception {
		Double idf = value.f0;
		int scrollSize = 10000;

		SearchRequestBuilder builder = App.esService
				.getClient()
				.prepareSearch(
						Config.getInstance().getString(Config.TERM_INDEX))
				.setScroll(new TimeValue(60000))
				.setTypes(Config.getInstance().getString(Config.TERM))
				.storedFields("tf")
				.setQuery(QueryBuilders.termQuery("term", value.f1));
		System.out.println(builder.toString());
		SearchResponse response = builder.setSize(scrollSize).execute()
				.actionGet();
		do {
			for (SearchHit hit : response.getHits().getHits()) {
				String id = hit.getId();
				Double tf = hit.field("tf").getValue();
				Double tfIdf = tf * idf;
				out.collect(new Tuple2<String, Double>(id, tfIdf));
			}
			response = App.esService.getClient()
					.prepareSearchScroll(response.getScrollId())
					.setScroll(new TimeValue(60000)).execute().actionGet();
		} while (response.getHits().getHits().length != 0);

	}

}
