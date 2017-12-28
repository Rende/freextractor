/**
 *
 */
package de.dfki.mlt.freextractor.flink;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class Entity {
	private String id;
	private String type;
	private String label;
	private String wikipediaTitle;
	private List<String> aliases;

	public Entity() {
		id = "";
		type = "";
		label = "";
		wikipediaTitle = "";
		aliases = new ArrayList<String>();
	}

	public Entity(String id, String type, String label, String wikipediaTitle,
			List<String> aliases) {
		this.id = id;
		this.type = type;
		this.label = label;
		this.wikipediaTitle = wikipediaTitle;
		this.aliases = aliases;
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @return the label
	 */
	public String getLabel() {
		return label;
	}

	/**
	 * @return the aliases
	 */
	public List<String> getAliases() {
		return aliases;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @param label
	 *            the label to set
	 */
	public void setLabel(String label) {
		this.label = label;
	}

	/**
	 * @param aliases
	 *            the aliases to set
	 */
	public void setAliases(List<String> aliases) {
		this.aliases = aliases;
	}

	/**
	 * @return the wikipediaTitle
	 */
	public String getWikipediaTitle() {
		return wikipediaTitle;
	}

	/**
	 * @param wikipediaTitle
	 *            the wikipediaTitle to set
	 */
	public void setWikipediaTitle(String wikipediaTitle) {
		this.wikipediaTitle = wikipediaTitle;
	}

	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}

}
