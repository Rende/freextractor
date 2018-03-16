/**
 *
 */
package de.dfki.mlt.freextractor.flink;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class WikiObject {
	private int position;
	private String label;

	public WikiObject(int position, String label) {
		this.position = position;
		this.label = label;
	}

	/**
	 * @return the position
	 */
	public int getPosition() {
		return position;
	}

	/**
	 * @return the label
	 */
	public String getLabel() {
		return label;
	}

	/**
	 * @param position
	 *            the position to set
	 */
	public void setPosition(int position) {
		this.position = position;
	}

	/**
	 * @param label
	 *            the label to set
	 */
	public void setLabel(String label) {
		this.label = label;
	}
}
