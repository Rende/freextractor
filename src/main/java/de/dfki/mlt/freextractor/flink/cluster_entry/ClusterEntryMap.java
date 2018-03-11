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
import de.dfki.mlt.freextractor.flink.WikiObject;

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
	private JTok jtok;
	private Entity subject;
	private HashMap<String, Entity> entityMap;
	private HashMap<String, Entity> entityParentMap;
	private Integer objPos;
	private Integer subjPos;
	private Integer objCount;

	// pageId, subjectId, title, sentence, tokenizedSentence
	@Override
	public void flatMap(Tuple5<Integer, String, String, String, String> value,
			Collector<ClusterEntry> out) throws Exception {
		List<WikiObject> objectList = getObjectList(value.f3);
		subject = App.esService.getEntity(value.f1);
		String tokSentence = removeSubject(value.f4);

		if (subject != null) {
			entityMap = collectEntities(subject.getClaims());
			entityParentMap = getEntityParentMap(objectList);
			for (Pair<String, String> pair : subject.getClaims()) {
				Entity property = entityMap.get(pair.getValue0());
				Entity object = entityMap.get(pair.getValue1());
				if (object != null && property != null) {
					ClusterId clusterId = getClusterKey(object, property,
							objectList);
					if (clusterId != null) {
						HashMap<String, Integer> hist = createHistogram(removeObjectByIndex(
								tokSentence, objCount));
						ClusterEntry entry = new ClusterEntry(clusterId,
								value.f4, value.f0, subjPos, objPos, hist);
						out.collect(entry);
					}
				}
			}
		}
	}

	public HashMap<String, Integer> createHistogram(String text) {
		HashMap<String, Integer> hist = new HashMap<String, Integer>();
		String punctutations = ".,:;&*!?[['''''']]|";
		for (String token : text.split(" ")) {
			if (!punctutations.contains(token) && !containsOnlyDigits(token)) {
				int count = 0;
				if (hist.containsKey(token)) {
					count = hist.get(token);
				}
				count++;
				hist.put(token, count);
			}
		}
		return hist;
	}

	public String removeSubject(String text) {
		text = text.replaceAll("[''']+.*?[''']+", "");
		return text;
	}

	public String removeObjectByIndex(String text, int index) {
		int count = 0;
		Pattern p = Pattern.compile("\\[\\[.*?\\]\\]");
		Matcher m = p.matcher(text);
		StringBuffer sb = new StringBuffer();
		while (m.find()) {
			if (count == index)
				m.appendReplacement(sb, "");
			count++;
		}
		m.appendTail(sb);
		return sb.toString();
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
			List<WikiObject> objectList) {
		List<String> entityIdList = new ArrayList<String>();
		List<String> parentIdList = new ArrayList<String>();

		for (Pair<String, String> subjClaim : subject.getClaims()) {
			if (subjClaim.getValue0().equals(INSTANCE_OF_RELATION)) {
				entityIdList.add(subject.getId());
				parentIdList.add(subjClaim.getValue1());
			}
			Entity object = entityMap.get(subjClaim.getValue1());
			if (object != null && object.getClaims() != null) {
				for (WikiObject obj : objectList) {
					if (obj.getLabel().equalsIgnoreCase(
							Helper.fromStringToWikilabel(object.getLabel()))) {
						List<Pair<String, String>> objClaims = object
								.getClaims();
						for (Pair<String, String> objClaim : objClaims) {
							if (objClaim.getValue0().equals(
									INSTANCE_OF_RELATION)) {
								entityIdList.add(object.getId());
								parentIdList.add(objClaim.getValue1());
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

	private ClusterId getClusterKey(Entity object, Entity property,
			List<WikiObject> objectList) {
		Entity subjParent = entityParentMap.get(subject.getId());
		String subjType = "";
		String objType = "";
		String relLabel = "";
		if (subjParent != null) {
			subjType = subjParent.getLabel();
		} else {
			subjType = subject.getLabel();
		}

		for (int i = 0; i < objectList.size(); i++) {
			WikiObject obj = objectList.get(i);
			if (obj.getLabel().equalsIgnoreCase(
					Helper.fromStringToWikilabel(object.getLabel()))) {
				Entity objParent = entityParentMap.get(object.getId());
				if (objParent != null) {
					objType = objParent.getLabel();
				} else {
					objType = object.getLabel();
				}
				subjType = Helper.fromLabelToKey(subjType);
				objType = Helper.fromLabelToKey(objType);
				relLabel = Helper.fromLabelToKey(property.getLabel());
				objPos = obj.getPosition();
				objCount = i;
				return new ClusterId(subjType, objType, relLabel);
			}
		}
		return null;
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
	 * subject : ''' abc ''' = single token. object: [[ abc ]] = single token.
	 * the positions are counted based on this schema.
	 **/
	public List<WikiObject> getObjectList(String text) {
		List<WikiObject> objectList = new ArrayList<WikiObject>();
		AnnotatedString annString = jtok.tokenize(text, "en");
		List<Token> tokens = Outputter.createTokens(annString);

		int index = 0;
		boolean inBracket = false;
		boolean isSubject = false;
		StringBuilder builder = new StringBuilder();
		for (Token token : tokens) {
			if (token.getImage().contains("'''") && !isSubject) {
				isSubject = true;
			} else if (token.getImage().contains("'''") && isSubject) {
				isSubject = false;
				subjPos = index;
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
				objectList.add(new WikiObject(index, label));
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
		subjPos = 0;
		objPos = 0;
		objCount = 0;
	}

}
