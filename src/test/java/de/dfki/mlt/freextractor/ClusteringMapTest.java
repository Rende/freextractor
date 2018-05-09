/**
 *
 */
package de.dfki.mlt.freextractor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import de.dfki.mlt.freextractor.flink.Helper;
import de.dfki.mlt.freextractor.flink.Word;
import de.dfki.mlt.freextractor.flink.cluster_entry.ClusterEntryMap;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class ClusteringMapTest {
	private ClusterEntryMap clusteringMap = new ClusterEntryMap();
	private Helper helper = new Helper();

	public ClusteringMapTest() {
		clusteringMap.open(null);
	}

	@Test
	public void testCreateHistogram() {
		String test = "''' saint esteben ''' be a [[ commune of france | commune ]] in"
				+ " the [[ pyrénées atlantique ]] [[ department of france | department ]] "
				+ "in south western [[ france ]] + - / * . & '''''' 123 '''''";
		HashMap<String, Integer> expected = new HashMap<String, Integer>();
		expected.put("saint", 1);
		expected.put("esteben", 1);
		expected.put("be", 1);
		expected.put("a", 1);
		expected.put("commune", 1);
		expected.put("in", 2);
		expected.put("france", 1);
		expected.put("the", 1);
		expected.put("pyrénées", 1);
		expected.put("atlantique", 1);
		expected.put("department", 1);
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
		String testSentence = "'' ''' black dog ''' '' be a song by english rock band "
				+ "[[ lead zeppelin ]] , the open track on they [[ lead zeppelin iv | fourth album ]] .";
		String expectedSentence = " be a song by english rock band [[ lead zeppelin ]] , "
				+ "the open track on they [[ lead zeppelin iv | fourth album ]] .";
		String actualSentence = clusteringMap.removeSubject(testSentence);
		assertThat(actualSentence).isEqualTo(expectedSentence);
	}

	@Test
	public void testGetObjectCleanSentence() {
		String test = "be a [[ commune of France | commune ]] in "
				+ "the [[ pyrénées-atlantique ]] [[ Departments of France | department ]] "
				+ "in south-western [[ France ]] .";
		String actual = clusteringMap.getObjectCleanSentence(test);
		String expected = "be a commune in the pyrénées-atlantique department in south-western France .";
		assertThat(actual).isEqualTo(expected);

		String testSentence = "the  be a process of [[ debt restructuring ]] by  "
				+ "that begin on january 14 , 2005 , and allow it to resume payment "
				+ "on 76 % of the [[ unite state dollar | we $ ]] 82 billion in "
				+ "[[ sovereign bond ]] s that default in 2001 at the depth of "
				+ "[[ argentine economic crisis | the worst economic crisis ]] in the nation ' s history .";

		String actualSentence = clusteringMap.getObjectCleanSentence(testSentence);
		String expectedSentece = "the  be a process of debt restructuring by  "
				+ "that begin on january 14 , 2005 , and allow it to resume payment "
				+ "on 76 % of the we dollar 82 billion in " + "sovereign bond s that default in 2001 at the depth of "
				+ "the worst economic crisis in the nation ' s history .";
		assertThat(actualSentence).isEqualTo(expectedSentece);
	}

	@Test
	public void testRemoveObjectByIndex() {
		String test = "be a [[ commune of France | commune ]] in "
				+ "the [[ pyrénées-atlantique ]] [[ Departments of France | department ]] "
				+ "in south-western [[ France ]] .";
		String expected = "be a  in " + "the [[ pyrénées-atlantique ]] [[ Departments of France | department ]] "
				+ "in south-western [[ France ]] .";
		String actual = clusteringMap.removeObjectByIndex(test, 0);
		assertThat(actual).isEqualTo(expected);
		String expected2 = "be a [[ commune of France | commune ]] in "
				+ "the  [[ Departments of France | department ]] " + "in south-western [[ France ]] .";
		String actual2 = clusteringMap.removeObjectByIndex(test, 1);
		assertThat(actual2).isEqualTo(expected2);
	}

	@Test
	public void testGetObjectList() {
		String test = "''' Saint-Esteben ''' be a [[ commune of France | commune ]] in "
				+ "the [[ pyrénées-atlantique ]] [[ Departments of France | department ]] "
				+ "in south-western [[ France ]] .";

		List<Word> sentenceItemList = helper.getWordList(test);
		List<Word> actual = clusteringMap.getObjectList(sentenceItemList);
		assertThat(actual).extracting("position").containsExactly(3, 6, 7, 10);
		assertThat(actual).extracting("surface").containsExactly("Commune_of_France", "Pyrénées-atlantique",
				"Departments_of_France", "France");
	}

	@Test
	public void testGetRelationPhrase() {
		String test = "''' Saint-Esteben ''' be a [[ commune of France | commune ]] in "
				+ "the ' '' [[ pyrénées-atlantique ]] [[ Departments of France | department ]] "
				+ "in south-western [[ France ]] .";

		List<Word> wordList = helper.getWordList(test);
		String expectedRelationPhrase = "be a commune in the";
		clusteringMap.subjectPosition = 0;
		clusteringMap.objectPosition = 6;
		String actualRelationPhrase = clusteringMap.getRelationPhrase(wordList);
		assertThat(actualRelationPhrase).isEqualTo(expectedRelationPhrase);

		String testNoSubject = "'' a b c '' [[ d | e ]] '' x y [[ z ]]";
		List<Word> words = helper.getWordList(testNoSubject);
		String expRelationPhrase = "a b c e x y";
		clusteringMap.subjectPosition = -1;
		clusteringMap.objectPosition = 9;
		String actlRelationPhrase = clusteringMap.getRelationPhrase(words);
		assertThat(actlRelationPhrase).isEqualTo(expRelationPhrase);
	}

	@Test
	public void testGetBagOfWords() {
		String text = "went to the grocery shop which might be gone to the";
		Set<String> bagOfWords = new HashSet<String>();
		bagOfWords.add("go");
		bagOfWords.add("grocery");
		bagOfWords.add("shop");
		bagOfWords.add("be");
		Set<String> actualBagOfWords = clusteringMap.getBagOfWords(text);
//		System.out.println(bagOfWords.equals(actualBagOfWords));
		assertEquals(bagOfWords, actualBagOfWords);
	}
}
