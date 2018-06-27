/**
 *
 */
package de.dfki.mlt.freextractor.flink.cluster_entry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import de.dfki.lt.tools.tokenizer.JTok;
import de.dfki.mlt.freextractor.App;
import de.dfki.mlt.freextractor.flink.Entity;
import de.dfki.mlt.freextractor.flink.Helper;
import de.dfki.mlt.freextractor.flink.Type;
import de.dfki.mlt.freextractor.flink.Word;
import de.dfki.mlt.munderline.MunderLine;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class ClusterEntryMap
		extends RichFlatMapFunction<Tuple5<Integer, String, String, String, String>, ClusterEntry> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private static final String INSTANCE_OF_RELATION = "P31";
	private static final String SUBCLASS_OF_RELATION = "P279";
	private static final String PUNCTUATIONS = "`.,:;&*!?[['''''']]|=+-/";
	public MunderLine munderLine;
	public JTok jtok;
	protected StanfordCoreNLP pipeline;
	private Entity subject;
	private HashMap<String, Entity> entityMap;
	private HashMap<String, Entity> entityParentMap;
	public Integer objectPosition;
	public Integer subjectPosition;
	public Integer objectIndexInSentence;

	// pageId, subjectId, title, sentence, tokenizedSentence
	@Override
	public void flatMap(Tuple5<Integer, String, String, String, String> value, Collector<ClusterEntry> out)
			throws Exception {
		List<Word> words = App.helper.getWordList(value.f3);
		List<Word> objectList = getObjectList(words);
		subject = App.esService.getEntity(value.f1);
		if (subject != null) {
			String tokenizedSentence = removeSubject(value.f4);
			entityMap = collectEntities(subject.getClaims());
			entityParentMap = getEntityParentMap(objectList);
			Entity subjectParent = entityParentMap.get(subject.getId());
			String subjectType = getEntityType(subjectParent, subject);
			if (subjectType != null) {
				for (HashMap<String, String> claim : subject.getClaims()) {
					Entity property = entityMap.get(claim.get("property-id"));
					Entity object = entityMap.get(claim.get("object-id"));
					if (object != null && property != null) {
						ClusterId clusterId = getClusterKey(subjectType, object, property, objectList);
						if (clusterId != null) {
							HashMap<String, Integer> histogram = createHistogram(
									removeObjectByIndex(tokenizedSentence, objectIndexInSentence));
							String relationPhrase = getRelationPhrase(words);
							ClusterEntry entry = new ClusterEntry(clusterId, value.f4, subject.getLabel(),
									object.getLabel(), relationPhrase, value.f0, subjectPosition, objectPosition,
									histogram, getBagOfWords(relationPhrase));
							out.collect(entry);
						} 
					}
				}
			}
		}
	}

	public HashMap<String, Integer> createHistogram(String text) {
		text = getObjectCleanSentence(text);
		HashMap<String, Integer> histogram = new HashMap<String, Integer>();

		for (String token : text.split(" ")) {
			if (isTextOnly(token)) {
				int count = 0;
				if (histogram.containsKey(token)) {
					count = histogram.get(token);
				}
				count++;
				histogram.put(token, count);
			}
		}
		return histogram;
	}

	public String getRelationPhrase(List<Word> words) {
		StringBuilder builder = new StringBuilder();
		for (int i = subjectPosition + 1; i < objectPosition; i++) {
			Word word = words.get(i);
			if (word.getPosition() == i) {
				if (word.getType() == Type.SUBJECT)
					// this case is very unlikely
					builder.append(word.getSurface().replaceAll("'''", "") + " ");
				else if (word.getType() == Type.OBJECT)
					builder.append(App.helper.getObjectEntryLabel(word.getSurface()) + " ");
				else if (isTextOnly(word.getSurface()))
					builder.append(word.getSurface() + " ");
			}
		}
		return builder.toString().trim();
	}

	public String removeSubject(String text) {
		text = text.replaceAll("' '", "");
		text = text.replaceAll("[''']+.*?[''']+", "");
		return text;
	}

	public String removeObjectByIndex(String text, int index) {
		int count = 0;
		Pattern pattern = Pattern.compile("\\[\\[.*?\\]\\]");
		Matcher matcher = pattern.matcher(text);
		StringBuffer buffer = new StringBuffer();
		while (matcher.find()) {
			if (count == index)
				matcher.appendReplacement(buffer, "");
			count++;
		}
		matcher.appendTail(buffer);
		return buffer.toString();
	}

	public String getObjectCleanSentence(String text) {
		text = text.replaceAll("\\$", "dollar");
		Pattern pattern = Pattern.compile("\\[\\[.*?\\]\\]");
		Matcher matcher = pattern.matcher(text);
		StringBuffer buffer = new StringBuffer();
		while (matcher.find()) {
			String object = App.helper.getCleanObject(text.substring(matcher.start(), matcher.end()));
			try {
				matcher.appendReplacement(buffer, object);
			} catch (IllegalArgumentException e) {
				System.err.println(text);
				App.LOG.info(e + " the sentence: " + text);
			}
		}
		matcher.appendTail(buffer);
		return buffer.toString();
	}

	private boolean containsOnlyDigits(final String value) {
		for (int i = 0; i < value.length(); i++) {
			if (!Character.isDigit(value.charAt(i))) {
				return false;
			}
		}
		return true;
	}

	private boolean isTextOnly(String text) {
		return !PUNCTUATIONS.contains(text) && !containsOnlyDigits(text);
	}

	public HashMap<String, Entity> getEntityParentMap(List<Word> sentenceObjectList) {
		List<String> entityIdList = new ArrayList<String>();
		List<String> parentIdList = new ArrayList<String>();

		for (HashMap<String, String> subjectClaim : subject.getClaims()) {
			if (subjectClaim.containsValue(INSTANCE_OF_RELATION) || subjectClaim.containsValue(SUBCLASS_OF_RELATION)) {
				entityIdList.add(subject.getId());
				parentIdList.add(subjectClaim.get("object-id"));
			}
			Entity object = entityMap.get(subjectClaim.get("object-id"));
			if (object != null && object.getClaims() != null) {
				for (Word sentenceObject : sentenceObjectList) {
					if (sentenceObject.getSurface().equalsIgnoreCase(Helper.fromStringToWikilabel(object.getLabel()))) {
						for (HashMap<String, String> objectClaim : object.getClaims()) {
							if (objectClaim.containsValue(INSTANCE_OF_RELATION)
									|| objectClaim.containsValue(SUBCLASS_OF_RELATION)) {
								entityIdList.add(object.getId());
								parentIdList.add(objectClaim.get("object-id"));
							}
						}
					}
				}
			}
		}
		List<Entity> parentList = new ArrayList<Entity>();
		if (!parentIdList.isEmpty()) {
			parentList = App.esService.getMultiEntities(parentIdList);
		}
		HashMap<String, Entity> objectParentMap = new HashMap<String, Entity>();
		for (int i = 0; i < parentIdList.size(); i++) {
			for (int j = 0; j < parentList.size(); j++) {
				if (parentList.get(j).getId().equals(parentIdList.get(i))) {
					objectParentMap.put(entityIdList.get(i), parentList.get(j));
				}
			}
		}
		return objectParentMap;
	}

	private ClusterId getClusterKey(String subjectType, Entity object, Entity property, List<Word> sentenceObjectList) {

		for (int i = 0; i < sentenceObjectList.size(); i++) {
			Word sentenceObject = sentenceObjectList.get(i);
			if (sentenceObject.getSurface().equalsIgnoreCase(Helper.fromStringToWikilabel(object.getLabel()))) {
				Entity objectParent = entityParentMap.get(object.getId());
				String objectType = getEntityType(objectParent, object);
				if (objectType != null) {
					String relationLabel = Helper.fromLabelToKey(property.getLabel());
					objectPosition = sentenceObject.getPosition();
					objectIndexInSentence = i;
					return new ClusterId(subjectType, objectType, relationLabel, property.getId());
				}
			}
		}
		return null;
	}

	private String getEntityType(Entity parentEntity, Entity entity) {
		if (parentEntity != null) {
			return Helper.fromLabelToKey(parentEntity.getLabel());
		} else {
			// App.LOG.info("No parent for object: " + entity.getId());
			return null;
		}
	}

	public HashMap<String, Entity> collectEntities(List<HashMap<String, String>> claimList) {
		List<String> idSet = new ArrayList<String>();
		for (HashMap<String, String> claim : claimList) {
			idSet.addAll(claim.values());
		}
		HashMap<String, Entity> entityMap = new HashMap<String, Entity>();
		if (!idSet.isEmpty()) {
			// entityMap contains properties + objects
			entityMap = entityListToMap(App.esService.getMultiEntities(idSet));
		}
		return entityMap;
	}

	private HashMap<String, Entity> entityListToMap(List<Entity> entityList) {
		HashMap<String, Entity> entityMap = new HashMap<String, Entity>();
		for (Entity entity : entityList) {
			entityMap.put(entity.getId(), entity);
		}
		return entityMap;
	}

	public List<Word> getObjectList(List<Word> sentenceItemList) {
		List<Word> objectList = new ArrayList<Word>();
		for (Word item : sentenceItemList) {
			if (item.getType().equals(Type.OBJECT)) {
				item.setSurface(App.helper.getObjectEntryLabel(item.getSurface()));
				objectList.add(item);
			} else if (item.getType().equals(Type.SUBJECT)) {
				subjectPosition = item.getPosition();
			}
		}
		return objectList;
	}

	public Set<String> getBagOfWords(String text) {
		text = text.toLowerCase();
		Set<String> bagOfWords = Arrays.asList(lemmatize(text).split(" ")).stream().collect(Collectors.toSet());
		return bagOfWords;
	}

	public String lemmatize(String documentText) {
		StringBuilder builder = new StringBuilder();
		Annotation document = new Annotation(documentText);
		this.pipeline.annotate(document);
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		for (CoreMap sentence : sentences) {
			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				String tag = token.get(PartOfSpeechAnnotation.class);
				if (tag.startsWith("VB") || tag.startsWith("NN") || tag.startsWith("IN")) {
					String image = token.get(LemmaAnnotation.class);
					image = replaceParantheses(image);
					builder.append(image + " ");
				}
			}
		}
		return builder.toString().trim();
	}

	public String replaceParantheses(String image) {
		return image = image.replaceAll("-lrb-", "\\(").replaceAll("-rrb-", "\\)").replaceAll("-lcb-", "\\{")
				.replaceAll("-rcb-", "\\}").replaceAll("-lsb-", "\\[").replaceAll("-rsb-", "\\]");
	}

	@Override
	public void open(Configuration parameters) {
		subjectPosition = -1;
		objectPosition = -1;
		objectIndexInSentence = -1;
		Properties props;
		props = new Properties();
		props.put("annotators", "tokenize, ssplit, pos, lemma");
		pipeline = new StanfordCoreNLP(props);
		try {
			munderLine = new MunderLine("EN_pipeline.conf");
			jtok = new JTok();
		} catch (ConfigurationException | IOException e) {
			e.printStackTrace();
		}

	}

}
