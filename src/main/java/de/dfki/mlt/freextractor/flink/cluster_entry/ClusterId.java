/**
 *
 */
package de.dfki.mlt.freextractor.flink.cluster_entry;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class ClusterId {
	private String subjectType;
	private String objectType;
	private String relationLabel;

	public ClusterId(String subjectType, String objectType, String relationLabel) {
		this.subjectType = subjectType;
		this.objectType = objectType;
		this.relationLabel = relationLabel;
	}

	/**
	 * @return the subjectType
	 */
	public String getSubjectType() {
		return subjectType;
	}

	/**
	 * @return the objectType
	 */
	public String getObjectType() {
		return objectType;
	}

	/**
	 * @return the relationLabel
	 */
	public String getRelationLabel() {
		return relationLabel;
	}

	/**
	 * @param subjectType
	 *            the subjectType to set
	 */
	public void setSubjectType(String subjectType) {
		this.subjectType = subjectType;
	}

	/**
	 * @param objectType
	 *            the objectType to set
	 */
	public void setObjectType(String objectType) {
		this.objectType = objectType;
	}

	/**
	 * @param relationLabel
	 *            the relationLabel to set
	 */
	public void setRelationLabel(String relationLabel) {
		this.relationLabel = relationLabel;
	}

	public String toString() {
		return this.subjectType + " " + this.objectType + " "
				+ this.relationLabel;
	}

}
