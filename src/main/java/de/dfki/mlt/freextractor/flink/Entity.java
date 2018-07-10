/**
 *
 */
package de.dfki.mlt.freextractor.flink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class Entity {
	private String id;
	private String type;
	private String label;
	private String tokLabel;
	private String wikiTitle;
	private List<String> aliases;
	private List<String> tokAliases;
	private List<HashMap<String, String>> claims;

	public Entity() {
		id = "";
		type = "";
		label = "";
		tokLabel = "";
		wikiTitle = "";
		aliases = new ArrayList<String>();
		tokAliases = new ArrayList<String>();
		claims = new ArrayList<HashMap<String, String>>();
	}

	public Entity(String id, String type, String label, String tokLabel, String wikipediaTitle, List<String> aliases,
			List<String> tokAliases, List<HashMap<String, String>> claims) {
		this.id = id;
		this.type = type;
		this.label = label;
		this.tokLabel = tokLabel;
		this.wikiTitle = wikipediaTitle;
		this.aliases = aliases;
		this.tokAliases = tokAliases;
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
	public List<HashMap<String, String>> getClaims() {
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
	public void setClaims(List<HashMap<String, String>> claims) {
		this.claims = claims;
	}

	/**
	 * @return the tokLabel
	 */
	public String getTokLabel() {
		return tokLabel;
	}

	/**
	 * @return the tokAliases
	 */
	public List<String> getTokAliases() {
		return tokAliases;
	}

	/**
	 * @param tokLabel
	 *            the tokLabel to set
	 */
	public void setTokLabel(String tokLabel) {
		this.tokLabel = tokLabel;
	}

	/**
	 * @param tokAliases
	 *            the tokAliases to set
	 */
	public void setTokAliases(List<String> tokAliases) {
		this.tokAliases = tokAliases;
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Id: " + this.id + " Label: " + this.label + " WikiTitle: " + this.wikiTitle + " \nAliases \n");
		for (String alias : this.aliases) {
			builder.append(alias + "\n");
		}
		builder.append("Claims\n");
		for (HashMap<String, String> claimMap : this.claims) {
			for (Entry<String, String> claim : claimMap.entrySet()) {
				builder.append(claim.getKey() + ": " + claim.getValue() + "\n");
			}
		}
		return builder.toString();
	}
}
