/**
 *
 */
package de.dfki.mlt.diretc.flink.type_cluster;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class TypeClusterMember implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ClusterId clusterId;
	private String sentence;
	private String tokenizedSentence;
	private String subjectName;
	private String subjectId;
	private String objectName;
	private String objectId;
	private String relationId;
	private String relationPhrase;
	private Integer pageId;
	private Integer subjectPosition;
	private Integer objectPosition;
	private HashMap<String, Integer> histogram;
	private Set<String> bagOfWords;
	private Boolean isClusterMember;

	public TypeClusterMember(ClusterId id,String sentence, String tokenizedSentence, String subjectName, String subjectId, String objectName,
			String objectId, String relationId, String relationPhrase, Integer pageId, Integer subjectPosition,
			Integer objectPosition, HashMap<String, Integer> histogram, Set<String> bagOfWords) {

		this.clusterId = id;
		this.sentence = sentence;
		this.tokenizedSentence = tokenizedSentence;
		this.subjectName = subjectName;
		this.subjectId = subjectId;
		this.objectName = objectName;
		this.objectId = objectId;
		this.relationId = relationId;
		this.relationPhrase = relationPhrase;
		this.pageId = pageId;
		this.subjectPosition = subjectPosition;
		this.objectPosition = objectPosition;
		this.histogram = histogram;
		this.bagOfWords = bagOfWords;
		this.isClusterMember = false;
	}


	public String getTokenizedSentence() {
		return tokenizedSentence;
	}

	public Integer getPageId() {
		return pageId;
	}

	public void setTokenizedSentence(String tokenizedSentence) {
		this.tokenizedSentence = tokenizedSentence;
	}

	public void setPageId(Integer pageId) {
		this.pageId = pageId;
	}

	public ClusterId getClusterId() {
		return clusterId;
	}

	public void setClusterId(ClusterId clusterId) {
		this.clusterId = clusterId;
	}

	public Integer getSubjectPosition() {
		return subjectPosition;
	}

	public Integer getObjectPosition() {
		return objectPosition;
	}

	public void setSubjectPosition(Integer subjPos) {
		this.subjectPosition = subjPos;
	}

	public void setObjectPosition(Integer objPos) {
		this.objectPosition = objPos;
	}

	public HashMap<String, Integer> getHistogram() {
		return histogram;
	}

	public void setHistogram(HashMap<String, Integer> hist) {
		this.histogram = hist;
	}

	public String getSubjectName() {
		return subjectName;
	}

	public void setSubjectName(String subjectName) {
		this.subjectName = subjectName;
	}

	public String getObjectName() {
		return objectName;
	}

	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}

	public String getRelationPhrase() {
		return relationPhrase;
	}

	public void setRelationPhrase(String relationPhrase) {
		this.relationPhrase = relationPhrase;
	}

	public Set<String> getBagOfWords() {
		return bagOfWords;
	}

	public void setBagOfWords(Set<String> bagOfWords) {
		this.bagOfWords = bagOfWords;
	}

	public Boolean getIsClusterMember() {
		return isClusterMember;
	}

	public String getSubjectId() {
		return subjectId;
	}

	public void setSubjectId(String subjectId) {
		this.subjectId = subjectId;
	}

	public String getObjectId() {
		return objectId;
	}

	public void setObjectId(String objectId) {
		this.objectId = objectId;
	}

	public String getRelationId() {
		return relationId;
	}

	public void setRelationId(String relationId) {
		this.relationId = relationId;
	}

	public String getSentence() {
		return sentence;
	}

	public void setSentence(String sentence) {
		this.sentence = sentence;
	}
}
