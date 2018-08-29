/**
 *
 */
package de.dfki.mlt.freextractor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import de.dfki.mlt.freextractor.flink.Entity;
import de.dfki.mlt.freextractor.flink.Helper;
import de.dfki.mlt.freextractor.flink.Word;
import de.dfki.mlt.freextractor.flink.cluster_entry.ClusterEntryMap;
import de.dfki.mlt.freextractor.flink.cluster_entry.ClusterId;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class ClusterEntryMapTest {
	private ClusterEntryMap clusteringMap = new ClusterEntryMap();
	private Helper helper = new Helper();

	public ClusterEntryMapTest() {
		Configuration config = new Configuration();
		config.setString("lang", "en");
		clusteringMap.open(config);
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
		String test = "be a [[ commune of France | commune ]] in the [[ pyrénées-atlantique ]] "
				+ "[[ Departments of France | department ]] in south-western [[ France ]] .";
		String actual = clusteringMap.getObjectCleanSentence(test);
		String expected = "be a commune in the pyrénées-atlantique department in south-western France .";
		assertThat(actual).isEqualTo(expected);

		String testSentence = "the be a process of [[ debt restructuring ]] by "
				+ "that begin on january 14 , 2005 , and allow it to resume payment "
				+ "on 76 % of the [[ unite state dollar | we $ ]] 82 billion in "
				+ "[[ sovereign bond ]] s that default in 2001 at the depth of "
				+ "[[ argentine economic crisis | the worst economic crisis ]] in the nation ' s history .";

		String actualSentence = clusteringMap.getObjectCleanSentence(testSentence);
		String expectedSentece = "the be a process of debt restructuring by "
				+ "that begin on january 14 , 2005 , and allow it to resume payment "
				+ "on 76 % of the we dollar 82 billion in sovereign bond s that default in 2001 at the depth of "
				+ "the worst economic crisis in the nation ' s history .";
		assertThat(actualSentence).isEqualTo(expectedSentece);
	}

	@Test
	public void testRemoveObjectByIndex() {
		String test = "be a [[ commune of France | commune ]] in "
				+ "the [[ pyrénées-atlantique ]] [[ Departments of France | department ]] "
				+ "in south-western [[ France ]] .";
		String expected = "be a  in the [[ pyrénées-atlantique ]] [[ Departments of France | department ]] "
				+ "in south-western [[ France ]] .";
		String actual = clusteringMap.removeObjectByIndex(test, 0);
		assertThat(actual).isEqualTo(expected);
		String expected2 = "be a [[ commune of France | commune ]] in "
				+ "the  [[ Departments of France | department ]] in south-western [[ France ]] .";
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
		String test = "''''' Mr. Willowby ’ s Christmas Tree ''''' is a "
				+ "[[ television ]] [[ Christmas by medium | Christmas special ]]"
				+ " that first aired December 6 , 1995 on [[ CBS ]] .";

		List<Word> wordList = helper.getWordList(test);
		String expectedRelationPhrase = "is a television Christmas special that first aired December on";
		clusteringMap.subjectPosition = 0;
		clusteringMap.objectPosition = 13;
		List<String> relPhrases = clusteringMap.getRelationPhrases(wordList);
		String actualRelationPhrase = clusteringMap.getRelationPhraseAsString(relPhrases);
		assertThat(actualRelationPhrase).isEqualTo(expectedRelationPhrase);
	}

	@Test
	public void testRelationPhrase() {
		String testNoSubject = "''' NUS Business School ''' is the [[ business school ]] of the [[ National University of Singapore ]] .";
		List<Word> words = helper.getWordList(testNoSubject);
		String expRelationPhrase = "is the business school of the";
		clusteringMap.subjectPosition = 0;
		clusteringMap.objectPosition = 6;
		List<String> relPhrases = clusteringMap.getRelationPhrases(words);
		String actlRelationPhrase = clusteringMap.getRelationPhraseAsString(relPhrases);
		assertThat(actlRelationPhrase).isEqualTo(expRelationPhrase);
		Set<String> bagOfWords = new HashSet<String>();
		bagOfWords.add("be");
		bagOfWords.add("business");
		bagOfWords.add("school");
		bagOfWords.add("of");
		Set<String> actualBagOfWords = clusteringMap.getBagOfWords(relPhrases);
		assertEquals(bagOfWords, actualBagOfWords);
	}

	@Test
	public void testGetBagOfWords() {
		List<String> wordList = Arrays.asList("went to the Grocery Shop which might be gone to the".split(" "));
		Set<String> bagOfWords = new HashSet<String>();
		bagOfWords.add("go");
		bagOfWords.add("grocery");
		bagOfWords.add("shop");
		bagOfWords.add("be");
		Set<String> actualBagOfWords = clusteringMap.getBagOfWords(wordList);
		assertEquals(bagOfWords, actualBagOfWords);

		List<String> aliasList = Arrays.asList("Norwegian List of Lights ID".split(" "));
		Set<String> bow = new HashSet<String>();
		bow.add("list");
		bow.add("of");
		bow.add("light");
		bow.add("id");
		Set<String> actualBow = clusteringMap.getBagOfWords(aliasList);
		assertEquals(bow, actualBow);
	}

	@Test
	public void testBagOfWords() {
		List<String> relationPhrases = Arrays
				.asList("be a television Christmas special that first air December on".split(" "));
		Set<String> bow = new HashSet<String>();
		String[] bowArray = { "that", "be", "television", "christmas", "air", "december", "on" };
		bow.addAll(Arrays.asList(bowArray));
		Set<String> actualBow = clusteringMap.getBagOfWords(relationPhrases);
		assertEquals(bow, actualBow);

	}

	@Test
	public void testClusterEntryMap() {
		String sentence = "''' NUS Business School ''' is the [[ business school ]] of the [[ National University of Singapore ]] .";
		System.out.println("Sentence: " + sentence);
		String tokenizedSentence = "''' nus business school ''' be the [[ business school ]] of the [[ national university of singapore ]] .";
		System.out.println("Tokenized Sentence: " + tokenizedSentence);
		List<Word> words = App.helper.getWordList(sentence);
		System.out.println("\nWord List");
		for (Word word : words) {
			System.out.println(word.toString());
		}
		System.out.println("\nObject List");
		List<Word> objectList = clusteringMap.getObjectList(words);
		for (Word word : objectList) {
			System.out.println(word.toString());
		}
		System.out.println();
		Entity subject = App.esService.getEntity("Q6955642");
		clusteringMap.subject = subject;
		System.out.println("Subject: " + subject.getLabel());

		String tokSentence = clusteringMap.removeSubject(tokenizedSentence);

		System.out.println("Subjectless tok-sentence: " + tokSentence + "\n");
		System.out.println("All entities of subject");
		HashMap<String, Entity> entityMap = clusteringMap.collectEntities(subject.getClaims());
		for (Entry<String, Entity> entry : entityMap.entrySet()) {
			System.out.println("Entity id: " + entry.getKey() + " entity label:" + entry.getValue().getLabel());
		}

		assertThat(entityMap).containsOnlyKeys("Q1143635", "Q334", "Q738236", "P17", "P131", "P361", "P31");

		clusteringMap.entityMap = entityMap;

		System.out.println("\nParents");
		HashMap<String, Entity> entityParentMap = clusteringMap.getEntityParentMap(objectList);
		for (Entry<String, Entity> entry : entityParentMap.entrySet()) {
			System.out.println("Entity id: " + entry.getKey() + " parent-entity id: " + entry.getValue().getId()
					+ " parent-entity label: " + entry.getValue().getLabel());
		}
		assertThat(entityParentMap).containsOnlyKeys("Q738236", "Q6955642", "Q1143635");
		clusteringMap.entityParentMap = entityParentMap;

		Entity subjectParent = entityParentMap.get(subject.getId());
		System.out.println(
				"\nSubject parent label: " + subjectParent.getLabel() + " subject parent id: " + subjectParent.getId());
		clusteringMap.subjectParent = subjectParent;

		String subjectType = clusteringMap.getEntityType(subjectParent);
		System.out.println("Subject type: " + subjectType + "\n");

		String[] expectedRelationPhrases = { "is the", "is the business school of the" };
		List<HashSet<String>> expectedBows = new ArrayList<HashSet<String>>();
		expectedBows.add(0, new HashSet<>(Arrays.asList("be")));
		expectedBows.add(1, new HashSet<>(Arrays.asList("be", "business", "school", "of")));
		int index = 0;
		for (HashMap<String, String> claim : subject.getClaims()) {
			// for (Entry<String, String> entry : claim.entrySet()) {
			// System.out.println("Claim " + entry.getKey() + ": " + entry.getValue());
			// }
			Entity property = entityMap.get(claim.get("property-id"));
			Entity object = entityMap.get(claim.get("object-id"));
			ClusterId clusterId = clusteringMap.createClusterId(object, property, objectList);
			if (clusterId != null) {
				System.out.println("Cluster id: " + clusterId.toString());
				// HashMap<String, Integer> histogram = clusteringMap.createHistogram(
				// clusteringMap.removeObjectByIndex(tokSentence,
				// clusteringMap.objectIndexInSentence));
				// for (Entry<String, Integer> hist : histogram.entrySet()) {
				// System.out.println("Word: " + hist.getKey() + " Count: " + hist.getValue());
				// }
				List<String> relationPhrases = clusteringMap.getRelationPhrases(words);
				String relPhraseAsString = clusteringMap.getRelationPhraseAsString(relationPhrases);
				System.out.println("Relation Phrase: " + relPhraseAsString);
				assertThat(relPhraseAsString).isEqualTo(expectedRelationPhrases[index]);
				Set<String> bow = clusteringMap.getBagOfWords(relationPhrases);
				assertEquals(expectedBows.get(index), bow);
				index++;
				System.out.println();
			}
		}
	}

}
