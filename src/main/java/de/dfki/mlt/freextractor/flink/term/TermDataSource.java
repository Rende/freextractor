/**
 *
 */
package de.dfki.mlt.freextractor.flink.term;

import java.util.Collection;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;

import de.dfki.mlt.freextractor.App;
import de.dfki.mlt.freextractor.preferences.Config;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class TermDataSource implements SourceFunction<Tuple2<Double, String>> {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	boolean isRunning = true;

	@SuppressWarnings("unchecked")
	@Override
	public void run(SourceContext<Tuple2<Double, String>> ctx) throws Exception {
		while (isRunning) {
			SearchResponse response = App.esService
					.getClient()
					.prepareSearch(
							Config.getInstance().getString(Config.TERM_INDEX))
					.setTypes(Config.getInstance().getString(Config.TERM))
					.setQuery(QueryBuilders.matchAllQuery())
					.addAggregation(
							AggregationBuilders.terms("ts").field("term")
									.size(Integer.MAX_VALUE))
					.setFetchSource(true).setExplain(false).execute()
					.actionGet();

			Long total = App.esService.getClusterNumber();
			Terms terms = response.getAggregations().get("ts");
			Collection<Terms.Bucket> buckets = (Collection<Bucket>) terms.getBuckets();
			for (Bucket bucket : buckets) {
				Long df = bucket.getDocCount();
				Double idf = Math.log10(total / df);
				ctx.collect(new Tuple2<Double, String>(idf, bucket
						.getKeyAsString()));
			}
			System.out.println("Over");
			break;
		}

	}

	@Override
	public void cancel() {
		// TODO Auto-generated method stub

	}

}
