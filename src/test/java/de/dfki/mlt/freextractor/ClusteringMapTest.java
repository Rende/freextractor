/**
 *
 */
package de.dfki.mlt.freextractor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import de.dfki.mlt.freextractor.flink.WikiObject;
import de.dfki.mlt.freextractor.flink.cluster_entry.ClusterEntryMap;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class ClusteringMapTest {
	private ClusterEntryMap clusteringMap = new ClusterEntryMap();

	public ClusteringMapTest() {
		clusteringMap.open(null);
	}

	@Test
	public void testCreateHistogram() {
		String test = "''' saint esteben ''' be a [[ commune of france | commune ]] in"
				+ " the [[ pyrénées atlantique ]] [[ department of france | department ]] "
				+ "in south western [[ france ]] . & '''''' 123 '''''";
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
	public void testRemoveSubject() {
		String test = "''' subject ''' abc '''''' 123 '''''' def";
		String expected = " abc  def";
		String actual = clusteringMap.removeSubject(test);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void testRemoveObjectByIndex() {
		String test = "be a [[ commune of France | commune ]] in "
				+ "the [[ pyrénées-atlantique ]] [[ Departments of France | department ]] "
				+ "in south-western [[ France ]] .";
		String expected = "be a  in "
				+ "the [[ pyrénées-atlantique ]] [[ Departments of France | department ]] "
				+ "in south-western [[ France ]] .";
		String actual = clusteringMap.removeObjectByIndex(test, 0);
		assertThat(actual).isEqualTo(expected);
		String expected2 = "be a [[ commune of France | commune ]] in "
				+ "the  [[ Departments of France | department ]] "
				+ "in south-western [[ France ]] .";
		String actual2 = clusteringMap.removeObjectByIndex(test, 1);
		assertThat(actual2).isEqualTo(expected2);
	}

	@Test
	public void testGetObjectMap() {
		String test = "''' Saint-Esteben ''' be a [[ commune of France | commune ]] in "
				+ "the [[ pyrénées-atlantique ]] [[ Departments of France | department ]] "
				+ "in south-western [[ France ]] .";

		List<WikiObject> actual = clusteringMap.getObjectList(test);
		assertThat(actual).extracting("position").containsExactly(3, 6, 7, 10);
		assertThat(actual).extracting("label").containsExactly(
				"Commune_of_France", "Pyrénées-atlantique",
				"Departments_of_France", "France");

	}
}
