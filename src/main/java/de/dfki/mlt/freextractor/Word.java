/**
 *
 */
package de.dfki.mlt.freextractor;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class Word {
	private int position;
	private String surface;
	private WordType type;

	public Word(int position, String surface, WordType type) {
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

	public WordType getType() {
		return type;
	}

	public void setType(WordType type) {
		this.type = type;
	}

	public String toString() {
		return "Word: \"" + this.surface + "\" position: " + this.position + " type: " + this.type;
	}
}
