/**
 *
 */
package de.dfki.mlt.diretc.flink.term;

import java.util.Collection;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;

import de.dfki.mlt.diretc.App;
import de.dfki.mlt.diretc.preferences.Config;

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
			Collection<Terms.Bucket> buckets = App.esService.getClusters();
			for (Bucket bucket : buckets) {
				if (bucket.getDocCount() >= Config.getInstance().getInt(Config.MIN_CLUSTER_SIZE)) {
					ctx.collect(bucket.getKeyAsString());
				}
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
