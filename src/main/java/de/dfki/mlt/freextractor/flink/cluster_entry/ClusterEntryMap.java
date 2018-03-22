/**
 *
 */
package de.dfki.mlt.freextractor.flink.cluster_entry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.javatuples.Pair;

import de.dfki.lt.tools.tokenizer.JTok;
import de.dfki.lt.tools.tokenizer.annotate.AnnotatedString;
import de.dfki.lt.tools.tokenizer.output.Outputter;
import de.dfki.lt.tools.tokenizer.output.Token;
import de.dfki.mlt.freextractor.App;
import de.dfki.mlt.freextractor.flink.Entity;
import de.dfki.mlt.freextractor.flink.Helper;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class ClusterEntryMap
		extends
		RichFlatMapFunction<Tuple5<Integer, String, String, String, String>, ClusterEntry> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private static final String INSTANCE_OF_RELATION = "P31";
	private static final String SUBCLASS_OF_RELATION = "P279";
	private JTok jtok;
	private Entity subject;
	private HashMap<String, Entity> entityMap;
	private HashMap<String, Entity> entityParentMap;
	private Integer objectPosition;
	private Integer subjectPosition;
	private Integer objectIndexInSentence;

	// pageId, subjectId, title, sentence, tokenizedSentence
	@Override
	public void flatMap(Tuple5<Integer, String, String, String, String> value,
			Collector<ClusterEntry> out) throws Exception {
		List<SentenceObject> objectList = getObjectList(value.f3);
		subject = App.esService.getEntity(value.f1);
		if (subject != null) {
			String tokSentence = removeSubject(value.f4);
			entityMap = collectEntities(subject.getClaims());
			entityParentMap = getEntityParentMap(objectList);
			Entity subjectParent = entityParentMap.get(subject.getId());
			String subjectType = getEntityType(subjectParent, subject);
			if (subjectType != null) {
				for (Pair<String, String> pair : subject.getClaims()) {
					Entity property = entityMap.get(pair.getValue0());
					Entity object = entityMap.get(pair.getValue1());
					if (object != null && property != null) {
						ClusterId clusterId = getClusterKey(subjectType,
								object, property, objectList);
						if (clusterId != null) {
							HashMap<String, Integer> hist = createHistogram(removeObjectByIndex(
									tokSentence, objectIndexInSentence));
							ClusterEntry entry = new ClusterEntry(clusterId,
									value.f4, value.f0, subjectPosition,
									objectPosition, hist);
							out.collect(entry);
						} else {
							// App.LOG.info("No cluster entry for subject id: "
							// + subject.getId() + " with object "
							// + object.getId());
						}
					}
				}
			}
		}
	}

	public HashMap<String, Integer> createHistogram(String text) {
		HashMap<String, Integer> histogram = new HashMap<String, Integer>();
		String punctutations = "`.,:;&*!?[['''''']]|";
		for (String token : text.split(" ")) {
			if (!punctutations.contains(token) && !containsOnlyDigits(token)) {
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

	private boolean containsOnlyDigits(final String value) {
		for (int i = 0; i < value.length(); i++) {
			if (!Character.isDigit(value.charAt(i))) {
				return false;
			}
		}
		return true;
	}

	public HashMap<String, Entity> getEntityParentMap(
			List<SentenceObject> sentenceObjectList) {
		List<String> entityIdList = new ArrayList<String>();
		List<String> parentIdList = new ArrayList<String>();

		for (Pair<String, String> subjectClaim : subject.getClaims()) {
			if (subjectClaim.getValue0().equals(INSTANCE_OF_RELATION)
					|| subjectClaim.getValue0().equals(SUBCLASS_OF_RELATION)) {
				entityIdList.add(subject.getId());
				parentIdList.add(subjectClaim.getValue1());
			}
			Entity object = entityMap.get(subjectClaim.getValue1());
			if (object != null && object.getClaims() != null) {
				for (SentenceObject sentenceObject : sentenceObjectList) {
					if (sentenceObject.getLabel().equalsIgnoreCase(
							Helper.fromStringToWikilabel(object.getLabel()))) {
						List<Pair<String, String>> objectClaims = object
								.getClaims();
						for (Pair<String, String> objectClaim : objectClaims) {
							if (objectClaim.getValue0().equals(
									INSTANCE_OF_RELATION)
									|| objectClaim.getValue0().equals(
											SUBCLASS_OF_RELATION)) {
								entityIdList.add(object.getId());
								parentIdList.add(objectClaim.getValue1());
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

	private ClusterId getClusterKey(String subjectType, Entity object,
			Entity property, List<SentenceObject> sentenceObjectList) {

		for (int i = 0; i < sentenceObjectList.size(); i++) {
			SentenceObject sentenceObject = sentenceObjectList.get(i);
			if (sentenceObject.getLabel().equalsIgnoreCase(
					Helper.fromStringToWikilabel(object.getLabel()))) {
				Entity objectParent = entityParentMap.get(object.getId());
				String objectType = getEntityType(objectParent, object);
				if (objectType != null) {
					objectType = Helper.fromLabelToKey(objectType);
					String relationLabel = Helper.fromLabelToKey(property
							.getLabel());
					objectPosition = sentenceObject.getPosition();
					objectIndexInSentence = i;
					return new ClusterId(subjectType, objectType, relationLabel);
				}
			}
		}
		return null;
	}

	private String getEntityType(Entity parentEntity, Entity entity) {
		if (parentEntity != null) {
			return parentEntity.getLabel();
		} else {
			// App.LOG.info("No parent for object: " + entity.getId());
			return null;
		}
	}

	public HashMap<String, Entity> collectEntities(
			List<Pair<String, String>> claimObjectList) {
		List<String> idSet = new ArrayList<String>();
		for (Pair<String, String> pair : claimObjectList) {
			// property-id
			idSet.add(pair.getValue0());
			// object-id
			idSet.add(pair.getValue1());
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

	/**
	 * returns the list of objects, sets the subject position in the sentence.
	 * subject: ''' abc ''' = single token. object: [[ abc ]] = single token.
	 * the positions are counted based on this schema.
	 **/
	public List<SentenceObject> getObjectList(String text) {
		List<SentenceObject> objectList = new ArrayList<SentenceObject>();
		AnnotatedString annotatedString = jtok.tokenize(text, "en");
		List<Token> tokens = Outputter.createTokens(annotatedString);

		int index = 0;
		boolean inBracket = false;
		boolean isSubject = false;
		StringBuilder builder = new StringBuilder();
		for (Token token : tokens) {
			if (token.getImage().contains("'''") && !isSubject) {
				isSubject = true;
			} else if (token.getImage().contains("'''") && isSubject) {
				isSubject = false;
				subjectPosition = index;
				index++;
			} else if (isSubject) {

			} else if (token.getType().equals("OCROCHE")) {
				builder = new StringBuilder();
				builder.append(token.getImage() + " ");
				inBracket = true;
			} else if (token.getType().equals("CCROCHE") && inBracket) {
				String label = builder.toString().replaceAll("\\[\\[", "")
						.trim();
				if (label.contains("|")) {
					String[] labelArr = label.split("\\|");
					try {
						label = labelArr[0];
					} catch (ArrayIndexOutOfBoundsException e) {
						label = builder.toString().replaceAll("\\[\\[", "")
								.trim().replace("\\|", "");
					}
				}
				label = Helper.fromStringToWikilabel(label);
				objectList.add(new SentenceObject(index, label));
				inBracket = false;
				index++;
			} else if (inBracket) {
				builder.append(token.getImage() + " ");
			} else {
				index++;
			}
		}
		return objectList;
	}

	@Override
	public void open(Configuration parameters) {
		try {
			jtok = new JTok();
		} catch (IOException e) {
			e.printStackTrace();
		}
		subjectPosition = 0;
		objectPosition = 0;
		objectIndexInSentence = 0;
	}

}
