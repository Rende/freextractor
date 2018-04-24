/**
 *
 */
package de.dfki.mlt.freextractor.flink;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class SentenceItem {
	private int position;
	private String surface;
	private Type type;

	public SentenceItem(int position, String surface, Type type) {
		this.position = position;
		this.surface = surface;
		this.type = type;
	}

	public int getPosition() {
		return position;
	}

	public String getSurface() {
		return surface;
	}

	public void setPosition(int position) {
		this.position = position;
	}

	public void setSurface(String surface) {
		this.surface = surface;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}
}
