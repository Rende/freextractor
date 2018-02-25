/**
 *
 */
package de.dfki.mlt.freextractor.flink;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class ClusterId {
	private String subjType;
	private String objType;
	private String relLabel;

	public ClusterId(String subjType, String objType, String relLabel) {
		this.subjType = subjType;
		this.objType = objType;
		this.relLabel = relLabel;
	}

	/**
	 * @return the subjType
	 */
	public String getSubjType() {
		return subjType;
	}

	/**
	 * @return the objType
	 */
	public String getObjType() {
		return objType;
	}

	/**
	 * @return the relLabel
	 */
	public String getRelLabel() {
		return relLabel;
	}

	/**
	 * @param subjType
	 *            the subjType to set
	 */
	public void setSubjType(String subjType) {
		this.subjType = subjType;
	}

	/**
	 * @param objType
	 *            the objType to set
	 */
	public void setObjType(String objType) {
		this.objType = objType;
	}

	/**
	 * @param relLabel
	 *            the relLabel to set
	 */
	public void setRelLabel(String relLabel) {
		this.relLabel = relLabel;
	}

}
