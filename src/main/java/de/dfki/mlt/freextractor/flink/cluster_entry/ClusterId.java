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
	private String relationId;

	public ClusterId(String subjectType, String objectType, String relationLabel, String relationId) {
		this.subjectType = subjectType;
		this.objectType = objectType;
		this.relationLabel = relationLabel;
		this.relationId = relationId;
	}

	public String getSubjectType() {
		return subjectType;
	}

	public String getObjectType() {
		return objectType;
	}

	public String getRelationLabel() {
		return relationLabel;
	}

	public String getRelationId() {
		return relationId;
	}

	public void setSubjectType(String subjectType) {
		this.subjectType = subjectType;
	}

	public void setObjectType(String objectType) {
		this.objectType = objectType;
	}

	public void setRelationLabel(String relationLabel) {
		this.relationLabel = relationLabel;
	}

	public void setRelationId(String relationId) {
		this.relationId = relationId;
	}

	public String toString() {
		return this.subjectType + " " + this.objectType + " " + this.relationLabel;
	}

}
