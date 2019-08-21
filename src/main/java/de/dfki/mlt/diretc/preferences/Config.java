/**
 *
 */
package de.dfki.mlt.diretc.preferences;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * @author Aydan Rende, DFKI
 *
 */
public final class Config {

	public static final String CLUSTER_NAME = "cluster.name";
	public static final String BULK_FLUSH_MAX_ACTIONS = "bulk.flush.max.actions";
	public static final String HOST = "host";
	public static final String PORT = "port";
	public static final String HTTP_PORT = "http.port";
	public static final String WIKIDATA_INDEX = "wikidata.index";
	public static final String WIKIDATA_ENTITY = "wikidata.entity.type";
	public static final String WIKIPEDIA_SENTENCE_INDEX = "wikipedia.sentence.index";
	public static final String TYPE_CLUSTER_INDEX = "type.cluster.index";
	public static final String TERM_INDEX = "term.index";
	public static final String WIKIPEDIA_SENTENCE = "wikipedia.sentence.type";
	public static final String TERM = "term.type";
	public static final String CLUSTER_MEMBER = "type.cluster.type";
	public static final String NUMBER_OF_SHARDS = "number_of_shards";
	public static final String NUMBER_OF_REPLICAS = "number_of_replicas";
	public static final String MIN_CLUSTER_SIZE = "cluster.size.min";
	public static final String SCROLL_SIZE = "scroll.size";
	public static final String LANG = "language";

	private static PropertiesConfiguration config;

	private Config() {

	}

	private static void loadProps() {
		try {
			config = new PropertiesConfiguration("config.properties");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
	}

	public static PropertiesConfiguration getInstance() {

		if (config == null) {
			loadProps();
		}
		return config;
	}

}
