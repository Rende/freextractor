/**
 *
 */
package de.dfki.mlt.freextractor.flink.cluster_entry;

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

import de.dfki.mlt.freextractor.App;
import de.dfki.mlt.freextractor.flink.Entity;
import de.dfki.mlt.freextractor.flink.Helper;
import de.dfki.mlt.freextractor.flink.SentenceItem;
import de.dfki.mlt.freextractor.flink.Type;

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
	private Entity subject;
	private HashMap<String, Entity> entityMap;
	private HashMap<String, Entity> entityParentMap;
	private Integer objectPosition;
	private Integer subjectPosition;
	private Integer objectIndexInSentence;

	// pageId, subjectId, title, sentence, tokenizedSentence
	@Override
	public void flatMap(Tuple5<Integer, String, String, String, String> value, Collector<ClusterEntry> out)
			throws Exception {
		List<SentenceItem> sentenceItemList = App.helper.getSentenceItemList(value.f3);
		List<SentenceItem> objectList = getObjectList(sentenceItemList);
		subject = App.esService.getEntity(value.f1);
		if (subject != null) {
			String tokenizedSentence = removeSubject(value.f4);
			entityMap = collectEntities(subject.getClaims());
			entityParentMap = getEntityParentMap(objectList);
			Entity subjectParent = entityParentMap.get(subject.getId());
			String subjectType = getEntityType(subjectParent, subject);
			if (subjectType != null) {
				for (Pair<String, String> pair : subject.getClaims()) {
					Entity property = entityMap.get(pair.getValue0());
					Entity object = entityMap.get(pair.getValue1());
					if (object != null && property != null) {
						ClusterId clusterId = getClusterKey(subjectType, object, property, objectList);
						if (clusterId != null) {
							HashMap<String, Integer> histogram = createHistogram(
									removeObjectByIndex(tokenizedSentence, objectIndexInSentence));
							ClusterEntry entry = new ClusterEntry(clusterId, value.f4, subject.getLabel(),
									object.getLabel(), value.f0, subjectPosition, objectPosition, histogram);
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
		text = getObjectCleanSentence(text);
		HashMap<String, Integer> histogram = new HashMap<String, Integer>();
		String punctutations = "`.,:;&*!?[['''''']]|=+-/";
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

	public String getObjectCleanSentence(String text) {
		text = text.replaceAll("\\$", "dollar");
		Pattern pattern = Pattern.compile("\\[\\[.*?\\]\\]");
		Matcher matcher = pattern.matcher(text);
		StringBuffer buffer = new StringBuffer();
		while (matcher.find()) {
			String object = App.helper.getCleanObjectLabel(text.substring(matcher.start(), matcher.end()), false);
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

	public HashMap<String, Entity> getEntityParentMap(List<SentenceItem> sentenceObjectList) {
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
				for (SentenceItem sentenceObject : sentenceObjectList) {
					if (sentenceObject.getSurface().equalsIgnoreCase(Helper.fromStringToWikilabel(object.getLabel()))) {
						List<Pair<String, String>> objectClaims = object.getClaims();
						for (Pair<String, String> objectClaim : objectClaims) {
							if (objectClaim.getValue0().equals(INSTANCE_OF_RELATION)
									|| objectClaim.getValue0().equals(SUBCLASS_OF_RELATION)) {
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

	private ClusterId getClusterKey(String subjectType, Entity object, Entity property,
			List<SentenceItem> sentenceObjectList) {

		for (int i = 0; i < sentenceObjectList.size(); i++) {
			SentenceItem sentenceObject = sentenceObjectList.get(i);
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

	public HashMap<String, Entity> collectEntities(List<Pair<String, String>> claimObjectList) {
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

	public List<SentenceItem> getObjectList(List<SentenceItem> sentenceItemList) {
		List<SentenceItem> objectList = new ArrayList<SentenceItem>();
		for (SentenceItem item : sentenceItemList) {
			if (item.getType().equals(Type.OBJECT)) {
				item.setSurface(App.helper.getCleanObjectLabel(item.getSurface(), true));
				objectList.add(item);
			} else if (item.getType().equals(Type.SUBJECT)) {
				subjectPosition = item.getPosition();
			}
		}
		return objectList;
	}

	@Override
	public void open(Configuration parameters) {
		subjectPosition = -1;
		objectPosition = -1;
		objectIndexInSentence = -1;
	}

}
