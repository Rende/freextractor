/**
 *
 */
package de.dfki.mlt.freextractor.flink.cluster_entry;

import java.util.HashMap;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class ClusterEntry {

	private ClusterId clusterId;
	private String tokenizedSentence;
	private Integer pageId;
	private Integer subjPos;
	private Integer objPos;
	private HashMap<String, Integer> hist;

	public ClusterEntry(ClusterId id, String tokenizedSentence, Integer pageId,
			Integer subjPos, Integer objPos, HashMap<String, Integer> hist) {

		this.clusterId = id;
		this.tokenizedSentence = tokenizedSentence;
		this.pageId = pageId;
		this.subjPos = subjPos;
		this.objPos = objPos;
		this.hist = hist;
	}

	/**
	 * @return the tokenizedSentence
	 */
	public String getTokenizedSentence() {
		return tokenizedSentence;
	}

	/**
	 * @return the pageId
	 */
	public Integer getPageId() {
		return pageId;
	}


	/**
	 * @param tokenizedSentence
	 *            the tokenizedSentence to set
	 */
	public void setTokenizedSentence(String tokenizedSentence) {
		this.tokenizedSentence = tokenizedSentence;
	}

	/**
	 * @param pageId
	 *            the pageId to set
	 */
	public void setPageId(Integer pageId) {
		this.pageId = pageId;
	}

	/**
	 * @return the clusterId
	 */
	public ClusterId getClusterId() {
		return clusterId;
	}

	/**
	 * @param clusterId
	 *            the clusterId to set
	 */
	public void setClusterId(ClusterId clusterId) {
		this.clusterId = clusterId;
	}

	/**
	 * @return the subjPos
	 */
	public Integer getSubjPos() {
		return subjPos;
	}

	/**
	 * @return the objPos
	 */
	public Integer getObjPos() {
		return objPos;
	}

	/**
	 * @param subjPos the subjPos to set
	 */
	public void setSubjPos(Integer subjPos) {
		this.subjPos = subjPos;
	}

	/**
	 * @param objPos the objPos to set
	 */
	public void setObjPos(Integer objPos) {
		this.objPos = objPos;
	}

	/**
	 * @return the hist
	 */
	public HashMap<String, Integer> getHist() {
		return hist;
	}

	/**
	 * @param hist the hist to set
	 */
	public void setHist(HashMap<String, Integer> hist) {
		this.hist = hist;
	}

}
