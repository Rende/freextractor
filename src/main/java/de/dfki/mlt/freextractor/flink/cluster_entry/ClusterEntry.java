/**
 *
 */
package de.dfki.mlt.freextractor.flink.cluster_entry;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class ClusterEntry implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ClusterId clusterId;
	private String tokenizedSentence;
	private String subjectName;
	private String objectName;
	private String relationPhrase;
	private Integer pageId;
	private Integer subjectPosition;
	private Integer objectPosition;
	private HashMap<String, Integer> histogram;
	private Set<String> bagOfWords;

	public ClusterEntry(ClusterId id, String tokenizedSentence, String subjectName, String objectName,
			String relationPhrase, Integer pageId, Integer subjectPosition, Integer objectPosition,
			HashMap<String, Integer> histogram, Set<String> bagOfWords) {

		this.clusterId = id;
		this.tokenizedSentence = tokenizedSentence;
		this.subjectName = subjectName;
		this.objectName = objectName;
		this.relationPhrase = relationPhrase;
		this.pageId = pageId;
		this.subjectPosition = subjectPosition;
		this.objectPosition = objectPosition;
		this.histogram = histogram;
		this.bagOfWords = bagOfWords;
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
}
