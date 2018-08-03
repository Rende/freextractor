/**
 * 
 */
package de.dfki.mlt.freextractor.flink.kmeans;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.deeplearning4j.clustering.cluster.Cluster;
import org.deeplearning4j.clustering.cluster.ClusterSet;
import org.deeplearning4j.clustering.cluster.Point;
import org.deeplearning4j.clustering.kmeans.KMeansClustering;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import de.dfki.mlt.freextractor.App;
import de.dfki.mlt.freextractor.flink.Entity;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class KMeansMap extends RichFlatMapFunction<String, Tuple2<String, String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final String cvsSplitBy = ",";
	private static String workingDirectory = "";
	private static HashMap<String, Integer> features = new HashMap<>();
	private static Integer rowCount = 0;
	private static Integer featureCount = 0;

	private int clusterCount = 0;
	private List<String> tupleList = new ArrayList<String>();

	@Override
	public void flatMap(String clusterName, Collector<Tuple2<String, String>> out) throws Exception {

		String clusterInOneString = countPhrases(clusterName);
		writeTuples(clusterName, clusterInOneString);
		List<Point> points = getPoints();
		// for (Point point : points) {
		// System.out.println(point.getLabel() + " " + point.getArray());
		// }
		List<Cluster> clusterList = applyKMeans(points);
		printClusters(clusterName, clusterList);

		Set<Integer> positiveClusters = new HashSet<Integer>();
		for (int i = 0; i < clusterList.size(); i++) {
			Cluster cluster = clusterList.get(i);
			for (Point point : cluster.getPoints()) {
				if (point.getLabel().startsWith("@")) {
					positiveClusters.add(i);
					break;
				}
			}
		}
		for (Integer clusId : positiveClusters) {
			Cluster cluster = clusterList.get(clusId);
			for (Point point : cluster.getPoints()) {
				if (!point.getLabel().startsWith("@")) {
					out.collect(new Tuple2<String, String>(clusterName, point.getId()));
				}
			}
		}
	}

	private void writeTuples(String clusterName, String clusterInOneString) throws IOException {
		// prepare the file
		String tuplesFile = workingDirectory + clusterName + "_tuples.csv";
		FileWriter fileWriter = new FileWriter(tuplesFile);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		bufferedWriter.write("subject-name,subject-id,feature-name,feature");
		bufferedWriter.newLine();
		bufferedWriter.write(clusterInOneString);
		bufferedWriter.close();
	}

	private String countPhrases(String clusterId) throws IOException {
		SearchResponse response = App.esService.getClusterEntryHits(clusterId);
		StringBuilder builder = new StringBuilder();
		boolean aliasesInserted = false;
		Set<String> insertedAliases = new HashSet<String>();
		// int clusCount = 0;
		do {
			for (SearchHit hit : response.getHits().getHits()) {
				String relationId = hit.getSource().get("relation-id").toString();
				if (!aliasesInserted) {
					Entity relation = App.esService.getEntity(relationId);
					for (String tokAlias : relation.getTokAliases()) {
						if (!tokAlias.isEmpty()) {
							// clusCount++;
							String sAlias = "@" + tokAlias.toLowerCase().replaceAll(",", "").replaceAll(" ", "_");
							if (!insertedAliases.contains(sAlias)) {
								String[] phrases = tokAlias.split(" ");
								builder.append(createTuples(Arrays.asList(phrases), sAlias, rowCount));
								rowCount++;
							}
						}
					}
					aliasesInserted = true;
				}
				String clusterEntryId = hit.getId();
				ArrayList<String> bow = (ArrayList<String>) hit.getSource().get("relation-phrase-bow");
				builder.append(createTuples(bow, clusterEntryId, rowCount));
				rowCount++;
			}
			response = App.esService.getClient().prepareSearchScroll(response.getScrollId())
					.setScroll(new TimeValue(60000)).execute().actionGet();
		} while (response.getHits().getHits().length != 0);
		// clusterCount = clusCount;
		return builder.toString();
	}

	private String createTuples(List<String> phrases, String sampleName, int currentRow) {
		StringBuilder builder = new StringBuilder();
		for (String phrase : phrases) {
			if (!phrase.isEmpty()) {
				int currentFeature = 0;
				if (features.containsKey(phrase)) {
					currentFeature = features.get(phrase);
				} else {
					currentFeature = featureCount;
					features.put(phrase, featureCount);
					featureCount++;
				}
				builder.append(sampleName + "," + currentRow + "," + phrase + "," + currentFeature + "\n");
				tupleList.add(sampleName + "," + currentRow + "," + currentFeature);
			}
		}
		return builder.toString();
	}

	private List<Point> getPoints() {
		int prevRow = 0;
		String prevName = "";
		List<Point> points = new ArrayList<>();
		double[] vector = new double[featureCount];
		for (int i = 0; i < tupleList.size(); i++) {
			String tuple = tupleList.get(i);
			String[] fields = tuple.split(cvsSplitBy);
			int row = Integer.parseInt(fields[1]);
			int col = Integer.parseInt(fields[2]);
			if (prevRow == row) {
				prevName = fields[0];
			} else {
				INDArray data = Nd4j.create(vector);
				Point point = new Point(data);
				point.setLabel(prevName);
				point.setId(prevName);
				points.add(point);
				prevRow = row;
				vector = new double[featureCount];
			}
			vector[col] = 1.0;
			if (i == tupleList.size() - 1) {
				INDArray data = Nd4j.create(vector);
				Point point = new Point(data);
				point.setLabel(prevName);
				points.add(point);
			}
		}
		return points;
	}

	private List<Cluster> applyKMeans(List<Point> points) throws IOException {

		int maxIterationCount = 10;
		String distanceFunction = "cosinesimilarity";
		KMeansClustering kmc = KMeansClustering.setup(clusterCount, maxIterationCount, distanceFunction);
		ClusterSet cs = kmc.applyTo(points);
		List<Cluster> clusterList = cs.getClusters();

		return clusterList;

	}

	private void printClusters(String clusterName, List<Cluster> clusterList) throws IOException {
		String clustersFile = workingDirectory + clusterName + "_clusters.txt";
		FileWriter fw = new FileWriter(clustersFile);
		BufferedWriter bw = new BufferedWriter(fw);

		bw.write("\nClusters:\n");
		for (Cluster c : clusterList) {
			Point center = c.getCenter();
			bw.write("Cluster center: " + center.toString() + "\n");

			for (Point point : c.getPoints()) {
				bw.write(point.getLabel() + "\n");
			}
		}
		bw.close();
	}

	@Override
	public void open(Configuration parameters) {
		workingDirectory = System.getProperty("user.dir") + "/results/";

	}
}
