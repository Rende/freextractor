/**
 *
 */
package de.dfki.mlt.freextractor.flink.relation_extraction;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class Relation {

	// wikipedia page id: sentence-based
	private int pageId;
	private String subjId;
	// subjectIndex: subject's position in the sentence
	private int subjIndex;
	private String objId;
	// objectIndex: object's position in the sentence
	private int objIndex;
	// surface: sentence fragment represents the relation btw subj - obj
	private String surface;
	// startIndex: surface's start position
	private int startIndex;
	// endIndex: surface's end position
	private int endIndex;
	private String propId;
	private String alias;

	public Relation(int pageId, String subjectId, int subjectIndex,
			String objectId, int objectIndex, String surface, int startIndex,
			int endIndex, String propId, String alias) {
		this.pageId = pageId;
		this.subjId = subjectId;
		this.subjIndex = subjectIndex;
		this.objId = objectId;
		this.objIndex = objectIndex;
		this.surface = surface;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.propId = propId;
		this.alias = alias;
	}

	/**
	 * @return the subjectId
	 */
	public String getSubjectId() {
		return subjId;
	}

	/**
	 * @return the subjectIndex
	 */
	public int getSubjectIndex() {
		return subjIndex;
	}

	/**
	 * @return the objectId
	 */
	public String getObjectId() {
		return objId;
	}

	/**
	 * @return the objectIndex
	 */
	public int getObjectIndex() {
		return objIndex;
	}

	/**
	 * @return the surface
	 */
	public String getSurface() {
		return surface;
	}

	/**
	 * @return the startIndex
	 */
	public int getStartIndex() {
		return startIndex;
	}

	/**
	 * @return the endIndex
	 */
	public int getEndIndex() {
		return endIndex;
	}

	/**
	 * @return the pageId
	 */
	public int getPageId() {
		return pageId;
	}

	/**
	 * @return the propId
	 */
	public String getPropId() {
		return propId;
	}

	/**
	 * @return the alias
	 */
	public String getAlias() {
		return alias;
	}

	/**
	 * @param propId
	 *            the propId to set
	 */
	public void setPropId(String propId) {
		this.propId = propId;
	}

	/**
	 * @param alias
	 *            the alias to set
	 */
	public void setAlias(String alias) {
		this.alias = alias;
	}
}
