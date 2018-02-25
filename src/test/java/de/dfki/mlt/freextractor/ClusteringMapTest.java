/**
 *
 */
package de.dfki.mlt.freextractor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;

import org.junit.Test;

import de.dfki.mlt.freextractor.flink.ClusteringMap;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class ClusteringMapTest {
	private ClusteringMap clusteringMap = new ClusteringMap();

	public ClusteringMapTest() {
		clusteringMap.open(null);
	}

	@Test
	public void testCreateHistogram() {
		String test = "''' saint esteben ''' be a [[ commune of france | commune ]] in"
				+ " the [[ pyrénées atlantique ]] [[ department of france | department ]] "
				+ "in south western [[ france ]] .";
		HashMap<String, Integer> expected = new HashMap<String, Integer>();
		expected.put("saint", 1);
		expected.put("esteben", 1);
		expected.put("be", 1);
		expected.put("a", 1);
		expected.put("commune", 2);
		expected.put("in", 2);
		expected.put("of", 2);
		expected.put("france", 3);
		expected.put("the", 1);
		expected.put("pyrénées", 1);
		expected.put("atlantique", 1);
		expected.put("department", 2);
		expected.put("south", 1);
		expected.put("western", 1);
		HashMap<String, Integer> actual = clusteringMap.createHistogram(test);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void testGetObjectMap() {
		String test = "''' Saint-Esteben ''' be a [[ commune of France | commune ]] in "
				+ "the [[ pyrénées-atlantique ]] [[ Departments of France | department ]] "
				+ "in south-western [[ France ]] .";
		HashMap<Integer, String> expected = new HashMap<Integer, String>();
		expected.put(4, "Commune_of_France");
		expected.put(7, "Pyrénées-atlantique");
		expected.put(8, "Departments_of_France");
		expected.put(11, "France");
		HashMap<Integer, String> actual = clusteringMap.getObjectMap(test);
		assertThat(actual).isEqualTo(expected);

	}
}
