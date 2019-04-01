/**
 *
 */
package de.dfki.mlt.freextractor.flink.type_cluster;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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

import de.dfki.mlt.freextractor.App;
import de.dfki.mlt.freextractor.Entity;
import de.dfki.mlt.freextractor.Helper;
import de.dfki.mlt.freextractor.Word;
import de.dfki.mlt.freextractor.WordType;
import de.dfki.mlt.freextractor.preferences.Config;
import de.dfki.mlt.munderline.MunderLine;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import opennlp.tools.lemmatizer.LemmatizerME;
import opennlp.tools.lemmatizer.LemmatizerModel;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class TypeClusterMap
		extends RichFlatMapFunction<Tuple5<Integer, List<String>, String, String, String>, TypeClusterMember> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private static final String INSTANCE_OF_RELATION = "P31";
	private static final String SUBCLASS_OF_RELATION = "P279";
	private static final String PUNCTUATIONS = "`.,:;&*!?[['''''']]|=+-/";

	private StanfordCoreNLP pipeline;
	private LemmatizerME lemmatizer;
	private MunderLine munderLine;

	public String lang;
	// subj/obj position in the sentence
	public Integer objectPos;
	public Integer subjectPos;
	// object index in the object list
	public Integer objectIndex;

	// pageId, candidateSubjectIds, title, sentence, lemmatizedSentence
	@Override
	public void flatMap(Tuple5<Integer, List<String>, String, String, String> value, Collector<TypeClusterMember> out)
			throws Exception {
		List<Word> words = App.helper.getWordList(value.f3, this.lang);
		List<Word> objectList = getObjectList(words);
		String tokenizedSentence = removeSubject(value.f4);
		List<String> firstCandSubjects = value.f1.stream().limit(500).collect(Collectors.toList());
		List<Entity> candidateSubjects = App.esService.getMultiEntities(firstCandSubjects);
		for (Entity subject : candidateSubjects) {
			resetGlobals();
			HashMap<String, Entity> entityMap = collectEntities(subject.getClaims());
			HashMap<String, Entity> entityParentMap = getEntityParentMap(objectList, subject, entityMap);
			Entity subjectParent = entityParentMap.get(subject.getId());
			// if subject has a parent we can proceed otherwise a cluster cannot be formed
			if (subjectParent == null)
				continue;
			for (HashMap<String, String> claim : subject.getClaims()) {
				Entity property = entityMap.get(claim.get("property-id"));
				Entity object = entityMap.get(claim.get("wikibase-item"));
				if (property == null || object == null || subject.getLabels().get(this.lang) == null
						|| object.getLabels().get(this.lang) == null)
					continue;
				ClusterId clusterId = createClusterId(object, property, objectList, entityParentMap, subjectParent);
				if (clusterId == null)
					continue;
				HashMap<String, Integer> histogram = createHistogram(
						removeObjectByIndex(tokenizedSentence, objectIndex));
				List<String> relationPhrases = getRelationPhrases(words);
				String relPhraseAsString = getRelationPhraseAsString(relationPhrases);
				TypeClusterMember entry = new TypeClusterMember(clusterId, value.f3, value.f4,
						subject.getLabels().get(this.lang), subject.getId(), object.getLabels().get(this.lang),
						object.getId(), property.getId(), relPhraseAsString, value.f0, this.subjectPos, this.objectPos,
						histogram, getBagOfWords(relationPhrases));
				out.collect(entry);
			}
		}
	}

	/**
	 * Histogram should not contain neither subject phrase nor object phrase
	 * histogram: word -> word count
	 * 
	 * @param text
	 * @return histogram
	 */

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

	/**
	 * Returns the text snippet between subject and object
	 * 
	 * @param words
	 * @return relationPhrase
	 */
	public String getRelationPhraseAsString(List<String> relationPhrases) {
		StringBuilder builder = new StringBuilder();
		for (String phrase : relationPhrases) {
			builder.append(phrase + " ");
		}
		return builder.toString().trim();
	}

	public List<String> getRelationPhrases(List<Word> words) {
		List<String> phrases = new ArrayList<String>();
		for (int i = this.subjectPos + 1; i < this.objectPos; i++) {
			Word word = words.get(i);
			if (word.getPosition() == i) {
				if (word.getType() == WordType.SUBJECT)
					phrases.add(word.getSurface().replaceAll("'''", ""));
				else if (word.getType() == WordType.OBJECT)
					phrases.add(App.helper.getCleanObject(word.getSurface()));
				else if (isTextOnly(word.getSurface()))
					phrases.add(word.getSurface());
			}
		}
		return phrases;
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

	/**
	 * Removes the object notations and creates the Wikipedia form of the sentence
	 * 
	 * @param text
	 * @return text
	 */
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

	/**
	 * Returns entity-id -> parent-entity map Entities are mapped if they appear in
	 * the sentence
	 * 
	 */
	public HashMap<String, Entity> getEntityParentMap(List<Word> sentenceObjectList, Entity subject,
			HashMap<String, Entity> entityMap) {
		List<String> entityIdList = new ArrayList<String>();
		List<String> parentIdList = new ArrayList<String>();

		for (HashMap<String, String> subjectClaim : subject.getClaims()) {
			if (subjectClaim.containsValue(INSTANCE_OF_RELATION) || subjectClaim.containsValue(SUBCLASS_OF_RELATION)) {
				entityIdList.add(subject.getId());
				parentIdList.add(subjectClaim.get("wikibase-item"));
			}
			Entity object = entityMap.get(subjectClaim.get("wikibase-item"));
			if (object != null && object.getClaims() != null) {
				for (Word sentenceObject : sentenceObjectList) {
					if (object.getLabels().get(this.lang) != null && sentenceObject.getSurface()
							.equalsIgnoreCase(Helper.fromStringToWikilabel(object.getLabels().get(this.lang)))) {
						for (HashMap<String, String> objectClaim : object.getClaims()) {
							if (objectClaim.containsValue(INSTANCE_OF_RELATION)
									|| objectClaim.containsValue(SUBCLASS_OF_RELATION)) {
								entityIdList.add(object.getId());
								parentIdList.add(objectClaim.get("wikibase-item"));
							}
						}
					}
				}
			}
		}
		List<Entity> parentList = new ArrayList<Entity>();
		if (!parentIdList.isEmpty()) {
			try {
				parentList = App.esService.getMultiEntities(parentIdList);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
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

	/**
	 * Creates cluster id if both subject-parent and object-parent are available
	 * Updates current objectPos (token position in the sentence) Updates current
	 * objectIndex (which object in the object list)
	 * 
	 */
	public ClusterId createClusterId(Entity object, Entity property, List<Word> sentenceObjectList,
			HashMap<String, Entity> entityParentMap, Entity subjectParent) {
		for (int i = 0; i < sentenceObjectList.size(); i++) {
			Word sentenceObject = sentenceObjectList.get(i);
			if (object.getLabels().get(this.lang) != null && sentenceObject.getSurface()
					.equalsIgnoreCase(Helper.fromStringToWikilabel(object.getLabels().get(this.lang)))) {
				Entity objectParent = entityParentMap.get(object.getId());
				String objectType = getEntityType(objectParent);
				if (objectType != null && property.getLabels().get(this.lang) != null) {
					String relationLabel = Helper.fromLabelToKey(property.getLabels().get(this.lang));
					String subjectType = getEntityType(subjectParent);
					this.objectPos = sentenceObject.getPosition();
					this.objectIndex = i;
					return new ClusterId(subjectType, objectType, relationLabel, property.getId());
				}
			}
		}
		return null;
	}

	/**
	 * Returns the formatted label of parent entity which is considered as the type
	 * of entity
	 * 
	 */
	public String getEntityType(Entity parentEntity) {
		if (parentEntity != null && parentEntity.getLabels().get(this.lang) != null) {
			return Helper.fromLabelToKey(parentEntity.getLabels().get(this.lang));
		}
		return null;
	}

	/**
	 * Collect all entities of the subject and returns entity-id -> entity map
	 * 
	 */
	public HashMap<String, Entity> collectEntities(List<HashMap<String, String>> claimList) {
		List<String> idSet = new ArrayList<String>();
		for (HashMap<String, String> claim : claimList) {
			if (claim.containsKey("property-id") && claim.containsKey("wikibase-item")) {
				idSet.add(claim.get("property-id"));
				idSet.add(claim.get("wikibase-item"));
			}
		}
		HashMap<String, Entity> entityMap = new HashMap<String, Entity>();
		if (!idSet.isEmpty()) {
			// entityMap contains properties + objects
			try {
				entityMap = entityListToMap(App.esService.getMultiEntities(idSet));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
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

	/**
	 * Returns a word list which consists of objects of the sentence
	 * 
	 */
	public List<Word> getObjectList(List<Word> sentenceItemList) {
		List<Word> objectList = new ArrayList<Word>();
		for (Word item : sentenceItemList) {
			if (item.getType().equals(WordType.OBJECT)) {
				Word objectWord = new Word(item.getPosition(), App.helper.getObjectEntryLabel(item.getSurface()),
						WordType.OBJECT);
				objectList.add(objectWord);
			} else if (item.getType().equals(WordType.SUBJECT)) {
				subjectPos = item.getPosition();
			}
		}
		return objectList;
	}

	/**
	 * Returns bag-of-words representation of a text
	 * 
	 */
	public Set<String> getBagOfWords(List<String> relationPhrases) {
		Set<String> bagOfWords = new HashSet<String>();
		if (this.lang.equals("en")) {
			bagOfWords = Arrays.asList(lemmatizeEN(relationPhrases).toLowerCase(Locale.ENGLISH).split(" ")).stream()
					.collect(Collectors.toSet());
		} else if (this.lang.equals("de")) {
			bagOfWords = Arrays.asList(lemmatizeDE(relationPhrases).toLowerCase(Locale.GERMAN).split(" ")).stream()
					.collect(Collectors.toSet());
		}
		return bagOfWords;
	}

	public String lemmatizeDE(List<String> tokensAsString) {
		String[][] coNllTable = this.munderLine.processTokenizedSentence(tokensAsString);

		String[] tokens = new String[coNllTable.length];
		String[] posTags = new String[coNllTable.length];
		for (int i = 0; i < coNllTable.length; i++) {
			tokens[i] = coNllTable[i][1];
			posTags[i] = coNllTable[i][3];
		}
		String[] lemmata = this.lemmatizer.lemmatize(tokens, posTags);
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < lemmata.length; i++) {
			if (checkAliasConditionDE(posTags[i])) {
				builder.append(lemmata[i] + " ");
			}
		}
		return builder.toString().trim();
	}

	public String lemmatizeEN(List<String> tokensAsString) {
		StringBuilder builder = new StringBuilder();
		Annotation document = null;
		for (String token : tokensAsString) {
			document = new Annotation(token);
			this.pipeline.annotate(document);
			List<CoreMap> sentences = document.get(SentencesAnnotation.class);
			for (CoreMap sentence : sentences) {
				for (CoreLabel coreLabel : sentence.get(TokensAnnotation.class)) {
					String image = coreLabel.get(LemmaAnnotation.class);
					String tag = coreLabel.get(PartOfSpeechAnnotation.class);
					if (checkAliasConditionEN(tag)) {
						image = replaceParantheses(image).toLowerCase();
						builder.append(image + " ");
					}
				}
			}
		}
		return builder.toString().trim();
	}

	/**
	 * Accept only English verbs, nouns and prepositions
	 * 
	 * @param tag
	 * @return
	 */
	private boolean checkAliasConditionEN(String tag) {
		return (tag.startsWith("VB") || tag.startsWith("NN") || tag.startsWith("IN"));
	}

	/**
	 * Accept only German (STTS tags) verbs, prepositions, nouns and some particles
	 * 
	 * @param tag
	 * @return
	 */
	private boolean checkAliasConditionDE(String tag) {
		return (tag.startsWith("V") || tag.startsWith("N") || tag.startsWith("APP") || tag.equals("PTKNEG")
				|| tag.equals("PTKREL") || tag.equals("PTKVZ") || tag.equals("PTKZU"));
	}

	public String replaceParantheses(String image) {
		return image = image.replaceAll("-lrb-", "\\(").replaceAll("-rrb-", "\\)").replaceAll("-lcb-", "\\{")
				.replaceAll("-rcb-", "\\}").replaceAll("-lsb-", "\\[").replaceAll("-rsb-", "\\]");
	}

	/**
	 * Lemmatizes the words, returns only verbs, nouns and prepositions
	 * 
	 * @param documentText
	 * @return lemmatizedText
	 */
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
					image = replaceBrackets(image);
					builder.append(image + " ");
				}
			}
		}
		return builder.toString().trim();
	}

	/**
	 * Replaces Penn TreeBank bracket codes with the actual characters
	 * 
	 * @param text
	 * @return text
	 */
	public String replaceBrackets(String text) {
		return text = text.replaceAll("-lrb-", "\\(").replaceAll("-rrb-", "\\)").replaceAll("-lcb-", "\\{")
				.replaceAll("-rcb-", "\\}").replaceAll("-lsb-", "\\[").replaceAll("-rsb-", "\\]");
	}

	/**
	 * Initializes the global variables
	 */
	@Override
	public void open(Configuration parameters) {
		this.subjectPos = -1;
		if (parameters.containsKey("lang")) {
			String lang = parameters.getString("lang", Config.getInstance().getString(Config.LANG));
			this.lang = lang;
		} else {
			this.lang = Config.getInstance().getString(Config.LANG);
		}

		if (this.lang.equals("en")) {
			initializeENModuls();
		} else if (this.lang.equals("de")) {
			initializeDEModuls();
		}
	}

	public void resetGlobals() {
		this.objectPos = -1;
		this.objectIndex = -1;
	}

	private void initializeENModuls() {
		Properties props;
		props = new Properties();
		props.put("annotators", "tokenize, ssplit, pos, lemma");
		this.pipeline = new StanfordCoreNLP(props);
	}

	private void initializeDEModuls() {
		LemmatizerModel lemmatizerModel = null;
		try {
			this.munderLine = new MunderLine("DE_pipeline.conf");
			String lemmatizerModelPath = "models/DE-lemmatizer.bin";
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(lemmatizerModelPath);
			if (null == in) {
				in = Files.newInputStream(Paths.get(lemmatizerModelPath));
			}
			lemmatizerModel = new LemmatizerModel(in);
		} catch (ConfigurationException | IOException e) {
			e.printStackTrace();
		}
		this.lemmatizer = new LemmatizerME(lemmatizerModel);
	}
}
