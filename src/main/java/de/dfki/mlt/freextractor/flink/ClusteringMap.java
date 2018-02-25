/**
 *
 */
package de.dfki.mlt.freextractor.flink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

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

/**
 * @author Aydan Rende, DFKI
 *
 */
public class ClusteringMap
		extends
		RichFlatMapFunction<Tuple5<Integer, String, String, String, String>, ClusterEntry> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private JTok jtok;
	private Entity subject;
	private HashMap<String, Entity> entityMap;
	private HashMap<String, Entity> entityParentMap;
	private Integer objPos;
	private Integer subjPos;

	// pageId, subjectId, title, sentence, tokenizedSentence
	@Override
	public void flatMap(Tuple5<Integer, String, String, String, String> value,
			Collector<ClusterEntry> out) throws Exception {
		HashMap<Integer, String> objectMap = getObjectMap(value.f3);
		subject = App.esService.getEntity(value.f1);

		HashMap<String, Integer> hist = createHistogram(value.f4);
		if (subject != null) {
			entityMap = collectEntities(subject.getClaims());
			entityParentMap = getEntityParentMap(objectMap);
			for (Pair<String, String> pair : subject.getClaims()) {
				Entity property = entityMap.get(pair.getValue0());
				Entity object = entityMap.get(pair.getValue1());
				if (object != null && property != null) {
					ClusterId clusterId = getClusterKey(object, property,
							objectMap);
					if (clusterId != null) {
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
		String punctutations = ".,:;*!?[['''''']]|";
		for (String token : text.split(" ")) {
			if (!punctutations.contains(token)) {
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

	public HashMap<String, Entity> getEntityParentMap(
			HashMap<Integer, String> objectMap) {
		List<String> entityIdList = new ArrayList<String>();
		List<String> parentIdList = new ArrayList<String>();

		for (Pair<String, String> subjClaim : subject.getClaims()) {
			if (subjClaim.getValue0().equals("P31")) {
				entityIdList.add(subject.getId());
				parentIdList.add(subjClaim.getValue1());
			}
			Entity object = entityMap.get(subjClaim.getValue1());
			if (object != null && object.getClaims() != null) {
				for (String objLabel : objectMap.values()) {
					if (objLabel.equalsIgnoreCase(Helper
							.fromStringToWikilabel(object.getLabel()))) {
						List<Pair<String, String>> objClaims = object
								.getClaims();
						for (Pair<String, String> objClaim : objClaims) {
							if (objClaim.getValue0().equals("P31")) {
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
			HashMap<Integer, String> objectMap) {
		Entity subjParent = entityParentMap.get(subject.getId());
		String subjType = "";
		String objType = "";
		if (subjParent != null) {
			subjType = subjParent.getLabel();
		} else {
			subjType = subject.getLabel();
		}

		for (Entry<Integer, String> entry : objectMap.entrySet()) {
			String objLabel = entry.getValue();
			if (objLabel.equalsIgnoreCase(Helper.fromStringToWikilabel(object
					.getLabel()))) {
				Entity objParent = entityParentMap.get(object.getId());
				if (objParent != null) {
					objType = objParent.getLabel();
				} else {
					objType = object.getLabel();
				}
				subjType = Helper.fromLabelToKey(subjType);
				objType = Helper.fromLabelToKey(objType);
				String relLabel = Helper.fromLabelToKey(property.getLabel());
				objPos = entry.getKey();
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

	public HashMap<Integer, String> getObjectMap(String text) {
		HashMap<Integer, String> objectMap = new HashMap<Integer, String>();
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
				index++;
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
				objectMap.put(index, label);
				inBracket = false;
				index++;
			} else if (inBracket) {
				builder.append(token.getImage() + " ");
			} else {
				index++;
			}
		}
		return objectMap;
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
	}

}
