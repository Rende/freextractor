/**
 *
 */
package de.dfki.mlt.freextractor.flink;

import java.util.ArrayList;
import java.util.List;

import org.javatuples.Pair;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class Entity {
	private String id;
	private String type;
	private String label;
	private String wikiTitle;
	private List<String> aliases;
	private List<Pair<String, String>> claims;

	public Entity() {
		id = "";
		type = "";
		label = "";
		wikiTitle = "";
		aliases = new ArrayList<String>();
		claims = new ArrayList<Pair<String, String>>();
	}

	public Entity(String id, String type, String label, String wikipediaTitle,
			List<String> aliases, List<Pair<String, String>> claims) {
		this.id = id;
		this.type = type;
		this.label = label;
		this.wikiTitle = wikipediaTitle;
		this.aliases = aliases;
		this.claims = claims;
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
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type
	 *            the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * @return the wikiTitle
	 */
	public String getWikiTitle() {
		return wikiTitle;
	}

	/**
	 * @return the claims
	 */
	public List<Pair<String, String>> getClaims() {
		return claims;
	}

	/**
	 * @param wikiTitle
	 *            the wikiTitle to set
	 */
	public void setWikiTitle(String wikiTitle) {
		this.wikiTitle = wikiTitle;
	}

	/**
	 * @param claims
	 *            the claims to set
	 */
	public void setClaims(List<Pair<String, String>> claims) {
		this.claims = claims;
	}

}
