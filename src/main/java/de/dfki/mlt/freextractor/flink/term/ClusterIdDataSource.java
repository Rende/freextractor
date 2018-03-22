/**
 *
 */
package de.dfki.mlt.freextractor.flink.term;

import java.util.Collection;

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
public class ClusterIdDataSource implements SourceFunction<String> {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private boolean isRunning = true;

	@Override
	public void run(SourceContext<String> ctx) throws Exception {

		while (isRunning) {
			SearchResponse response = App.esService
					.getClient()
					.prepareSearch(
							Config.getInstance().getString(
									Config.CLUSTER_ENTRY_INDEX))
					.setTypes(
							Config.getInstance()
									.getString(Config.CLUSTER_ENTRY))
					.setQuery(QueryBuilders.matchAllQuery())
					.addAggregation(
							AggregationBuilders.terms("clusters")
									.field("cluster-id").size(3310106))
					.setFetchSource(true).setExplain(false).execute()
					.actionGet();

			Terms terms = response.getAggregations().get("clusters");
			Collection<Terms.Bucket> buckets = terms.getBuckets();
			for (Bucket bucket : buckets) {
				// if (bucket.getDocCount() > 1)
				ctx.collect(bucket.getKeyAsString());
				// System.out.println(bucket.getKeyAsString() + " ("
				// + bucket.getDocCount() + ")");
			}
			System.out.println("Over");
			break;
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

}
