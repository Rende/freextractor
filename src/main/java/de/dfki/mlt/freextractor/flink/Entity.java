/**
 *
 */
package de.dfki.mlt.freextractor.flink;

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
	private String datatype;
	private HashMap<String, String> labels;
	private HashMap<String, String> lemLabels;
	private HashMap<String, String> descriptions;
	private HashMap<String, String> lemDescriptions;
	private HashMap<String, List<String>> aliases;
	private HashMap<String, List<String>> lemAliases;
	private List<HashMap<String, String>> claims;

	public Entity( String type, String datatype, HashMap<String, String> labels,
			HashMap<String, String> lemLabels, HashMap<String, String> descriptions,
			HashMap<String, String> lemDescriptions, HashMap<String, List<String>> aliases,
			HashMap<String, List<String>> lemAliases, List<HashMap<String, String>> claims) {
		
		this.id = "";
		this.type = type;
		this.datatype = datatype;
		this.labels = labels;
		this.lemLabels = lemLabels;
		this.descriptions = descriptions;
		this.lemDescriptions = lemDescriptions;
		this.aliases = aliases;
		this.lemAliases = lemAliases;
		this.claims = claims;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getDatatype() {
		return datatype;
	}

	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}

	public HashMap<String, String> getLabels() {
		return labels;
	}

	public void setLabels(HashMap<String, String> labels) {
		this.labels = labels;
	}

	public HashMap<String, String> getLemLabels() {
		return lemLabels;
	}

	public void setLemLabels(HashMap<String, String> lemLabels) {
		this.lemLabels = lemLabels;
	}

	public HashMap<String, String> getDescriptions() {
		return descriptions;
	}

	public void setDescriptions(HashMap<String, String> descriptions) {
		this.descriptions = descriptions;
	}

	public HashMap<String, String> getLemDescriptions() {
		return lemDescriptions;
	}

	public void setLemDescriptions(HashMap<String, String> lemDescriptions) {
		this.lemDescriptions = lemDescriptions;
	}

	public HashMap<String, List<String>> getAliases() {
		return aliases;
	}

	public void setAliases(HashMap<String, List<String>> aliases) {
		this.aliases = aliases;
	}

	public HashMap<String, List<String>> getLemAliases() {
		return lemAliases;
	}

	public void setLemAliases(HashMap<String, List<String>> lemAliases) {
		this.lemAliases = lemAliases;
	}

	public List<HashMap<String, String>> getClaims() {
		return claims;
	}

	public void setClaims(List<HashMap<String, String>> claims) {
		this.claims = claims;
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Id: " + this.id + " Label: " + this.labels.get("en") + " \nAliases \n");
		for (String alias : this.aliases.get("en")) {
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
