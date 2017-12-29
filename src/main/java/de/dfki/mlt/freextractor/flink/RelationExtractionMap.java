/**
 *
 */
package de.dfki.mlt.freextractor.flink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
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
public class RelationExtractionMap extends
		RichFlatMapFunction<Tuple4<Integer, String, String, String>, Relation> {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private JTok jtok;

	private HashMap<Integer, String> objectMap;
	private HashMap<String, Entity> entityMap;
	private HashMap<String, Entity> objectParentMap;
	private List<String> tokenList;
	private Entity subject;
	private int subjectIndex;

	// pageId, subjectId, title, sentence
	public void flatMap(Tuple4<Integer, String, String, String> value,
			Collector<Relation> out) throws Exception {

		// objectMap is created in tokenizer
		tokenList = tokenizer(value.f3, value.f2);
		subject = App.esService.getEntity(value.f1);
		if (subject != null) {
			entityMap = collectEntities(subject.getClaims());
			objectParentMap = getObjectParentMap();
			for (Pair<String, String> pair : subject.getClaims()) {
				Entity property = entityMap.get(pair.getValue0());
				Entity object = entityMap.get(pair.getValue1());
				if (object != null && property != null) {
					List<Relation> relationList = getRelationList(object,
							property, value.f1, value.f0);
					for (Relation relation : relationList) {
						out.collect(relation);
						System.out.println(relation.getSurface());
					}
				}
			}
		}
	}

	public HashMap<String, Entity> getObjectParentMap() {
		List<String> objIdList = new ArrayList<String>();
		List<String> idList = new ArrayList<String>();
		for (Pair<String, String> subjClaim : subject.getClaims()) {
			Entity object = entityMap.get(subjClaim.getValue1());
			if (object != null && object.getClaims() != null) {
				for (int objectIndex : objectMap.keySet()) {
					if (objectMap.get(objectIndex).equalsIgnoreCase(
							fromStringToWikilabel(object.getLabel()))) {
						List<Pair<String, String>> objClaims = object
								.getClaims();
						for (Pair<String, String> objClaim : objClaims) {
							if (objClaim.getValue0().equals("P31")) {
								objIdList.add(object.getId());
								idList.add(objClaim.getValue1());
							}
						}
					}
				}
			}
		}
		List<Entity> parentList = new ArrayList<Entity>();
		if (!idList.isEmpty()) {
			parentList = App.esService.getMultiEntities(idList);
		}
		HashMap<String, Entity> objectParentMap = new HashMap<String, Entity>();
		for (int i = 0; i < objIdList.size(); i++) {
			objectParentMap.put(objIdList.get(i), parentList.get(i));
		}
		return objectParentMap;

	}

	public List<Relation> getRelationList(Entity object, Entity property,
			String subjectId, int pageId) {
		List<Relation> relationList = new ArrayList<Relation>();
		for (int objectIndex : objectMap.keySet()) {
			if (objectMap.get(objectIndex).equalsIgnoreCase(
					fromStringToWikilabel(object.getLabel()))) {
				Relation relation = searchRelation(property, objectIndex,
						object.getId(), subjectId, pageId);
				if (relation != null) {
					relationList.add(relation);
				}
			}
		}
		return relationList;
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

	public List<String> tokenizer(String text, String title) {
		objectMap = new HashMap<Integer, String>();
		AnnotatedString annString = jtok.tokenize(text, "en");
		List<Token> tokenList = Outputter.createTokens(annString);
		List<String> wordList = new ArrayList<String>();

		int index = 0;
		int subjectCount = 0;
		boolean isSubject = false;
		boolean inBracket = false;
		StringBuilder builder = new StringBuilder();
		for (Token token : tokenList) {
			if (token.getImage().contains("'''") && !isSubject) {
				builder = new StringBuilder();
				builder.append(token.getImage() + " ");
				isSubject = true;
				subjectCount++;
			} else if (token.getImage().contains("'''") && isSubject) {
				isSubject = false;
				builder.append(token.getImage() + " ");
				wordList.add(index, builder.toString());
				// the index of first found subject
				// will be the valid subject index
				if (subjectCount == 1) {
					subjectIndex = index;
				}
				index++;
			} else if (isSubject) {
				builder.append(token.getImage() + " ");
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
				label = fromStringToWikilabel(label);
				objectMap.put(index, label);
				builder.append(token.getImage() + " ");
				inBracket = false;
				wordList.add(index, builder.toString());
				index++;
			} else if (inBracket) {
				builder.append(token.getImage() + " ");
			} else {
				wordList.add(index, token.getImage());
				index++;
			}
		}
		if (subjectCount == 0) {
			subjectIndex = 0;
			wordList.add(0, title);
		}
		return wordList;
	}

	public String fromStringToWikilabel(String image) {
		String label = "";
		if (image.contains("|")) {
			String[] images = image.split("\\|");
			try {
				label = images[0];
			} catch (ArrayIndexOutOfBoundsException e) {
				label = image.trim().replace("\\|", "");
			}
		} else {
			label = image;
		}
		label = StringUtils.capitalize(label.trim().replaceAll(" ", "_"));
		return label;
	}

	public Relation searchRelation(Entity property, int objectIndex,
			String objectId, String subjectId, int pageId) {
		appendParentLabel(objectIndex, objectId);
		for (String alias : property.getAliases()) {
			String[] aliasFragments = alias.split(" ");
			List<String> aliasFragmentList = new ArrayList<String>();
			for (int i = 0; i < aliasFragments.length; i++) {
				aliasFragmentList.add(" " + aliasFragments[i] + " ");
			}
			int startIndex = 0;
			int endIndex = 0;
			int index = 0;

			List<String> tempTokenList = aliasFragmentList;
			while (index < tokenList.size() && !tempTokenList.isEmpty()) {
				String token = " " + tokenList.get(index) + " ";
				String tempAliasFragment = tempTokenList.get(0);
				if (token.contains(tempAliasFragment)) {
					if (startIndex == 0)
						startIndex = index;
					tempTokenList.remove(0);
					endIndex = index;
				} else if (!tokenList.get(index).contains(tempTokenList.get(0))) {
					tempTokenList = aliasFragmentList;
					startIndex = 0;
					endIndex = 0;
				}
				index++;
			}
			if (startIndex != 0 && endIndex != 0) {
				StringBuilder builder = new StringBuilder();
				for (int i = startIndex; i <= endIndex; i++) {
					builder.append(tokenList.get(i) + " ");
				}
				// System.out.println("Relation:" + builder.toString().trim()
				// + " startIndex: " + startIndex + " endIndex:"
				// + endIndex);
				return new Relation(pageId, subjectId, subjectIndex, objectId,
						objectIndex, builder.toString().trim(), startIndex,
						endIndex, property.getId(), alias);
			}
		}
		return null;
	}

	private void appendParentLabel(int objectIndex, String objectId) {
		if (!objectParentMap.isEmpty() && objectParentMap.containsKey(objectId)) {
			String token = tokenList.get(objectIndex);
			if (token.contains("]]")) {
				token = token.replaceAll("\\]\\]", "");
			}
			token = token + "| " + objectParentMap.get(objectId).getLabel();
			if (token.contains("[[")) {
				token = token + " ]]";
			}
			tokenList.set(objectIndex, token);
		}
	}

	@Override
	public void open(Configuration parameters) {
		try {
			super.open(parameters);
			jtok = new JTok();
			objectMap = new HashMap<Integer, String>();
			entityMap = new HashMap<String, Entity>();
			objectParentMap = new HashMap<String, Entity>();
			tokenList = new ArrayList<String>();
			subject = new Entity();
			subjectIndex = 0;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
