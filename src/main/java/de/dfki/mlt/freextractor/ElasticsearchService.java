/**
 *
 */
package de.dfki.mlt.freextractor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.javatuples.Pair;

import de.dfki.mlt.freextractor.flink.Entity;
import de.dfki.mlt.freextractor.preferences.Config;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class ElasticsearchService {
	private Client client;

	public ElasticsearchService() {
		getClient();
	}

	public Client getClient() {
		if (client == null) {
			Settings settings = Settings
					.builder()
					.put(Config.CLUSTER_NAME,
							Config.getInstance().getString(Config.CLUSTER_NAME))
					.build();
			try {
				client = new PreBuiltTransportClient(settings)
						.addTransportAddress(new InetSocketTransportAddress(
								InetAddress.getByName("134.96.187.233"), 9300));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		return client;

	}

	public static Map<String, String> getUserConfig() {
		Map<String, String> config = new HashMap<>();
		config.put(Config.BULK_FLUSH_MAX_ACTIONS, Config.getInstance()
				.getString(Config.BULK_FLUSH_MAX_ACTIONS));
		config.put(Config.CLUSTER_NAME,
				Config.getInstance().getString(Config.CLUSTER_NAME));

		return config;
	}

	public static List<InetSocketAddress> getTransportAddresses() {
		List<InetSocketAddress> transports = new ArrayList<>();
		try {
			transports.add(new InetSocketAddress(InetAddress.getByName(Config
					.getInstance().getString(Config.HOST)), Config
					.getInstance().getInt(Config.PORT)));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return transports;
	}

	public boolean checkAndCreateIndex(String indexName) throws IOException,
			InterruptedException {
		boolean result = false;
		IndicesAdminClient indicesAdminClient = getClient().admin().indices();
		final IndicesExistsResponse indexExistReponse = indicesAdminClient
				.prepareExists(indexName).execute().actionGet();
		if (!indexExistReponse.isExists()) {
			result = createIndex(indicesAdminClient, indexName);
		}
		return result;
	}

	private boolean createIndex(IndicesAdminClient indicesAdminClient,
			String indexName) {
		final CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdminClient
				.prepareCreate(indexName).setSettings(
						Settings.builder()
								.put(Config.NUMBER_OF_SHARDS,
										Config.getInstance().getInt(
												Config.NUMBER_OF_SHARDS))
								.put(Config.NUMBER_OF_REPLICAS,
										Config.getInstance().getInt(
												Config.NUMBER_OF_REPLICAS)));
		CreateIndexResponse createIndexResponse = null;
		createIndexResponse = createIndexRequestBuilder.execute().actionGet();
		return createIndexResponse != null
				&& createIndexResponse.isAcknowledged();
	}

	public boolean putMappingForRelations() throws IOException {
		IndicesAdminClient indicesAdminClient = getClient().admin().indices();
		XContentBuilder mappingBuilder = XContentFactory
				.jsonBuilder()
				.startObject()
				.startObject(
						Config.getInstance().getString(
								Config.WIKIPEDIA_RELATION))
				.startObject("properties").startObject("page-id")
				.field("type", "integer").field("index", "true").endObject()
				.startObject("subject-id").field("type", "keyword")
				.field("index", "true").endObject()
				.startObject("subject-index").field("type", "integer")
				.field("index", "true").endObject().startObject("object-id")
				.field("type", "keyword").field("index", "true").endObject()
				.startObject("object-index").field("type", "integer")
				.field("index", "true").endObject().startObject("surface")
				.endObject().startObject("start-index")
				.field("type", "integer").field("index", "true").endObject()
				.startObject("end-index").field("type", "integer")
				.field("index", "true").endObject().startObject("property-id")
				.field("type", "keyword").field("index", "true").endObject()
				.startObject("alias").field("type", "text").endObject()
				.endObject() // properties
				.endObject()// documentType
				.endObject();

		App.LOG.debug("Mapping for wikipedia relations: "
				+ mappingBuilder.string());
		PutMappingResponse putMappingResponse = indicesAdminClient
				.preparePutMapping(
						Config.getInstance().getString(
								Config.WIKIPEDIA_RELATION_INDEX))
				.setType(
						Config.getInstance().getString(
								Config.WIKIPEDIA_RELATION))
				.setSource(mappingBuilder).execute().actionGet();
		return putMappingResponse.isAcknowledged();
	}

	public boolean putMappingForClusterEntry() throws IOException {
		IndicesAdminClient indicesAdminClient = getClient().admin().indices();
		XContentBuilder mappingBuilder = XContentFactory
				.jsonBuilder()
				.startObject()
				.startObject(
						Config.getInstance().getString(Config.CLUSTER_ENTRY))
				.startObject("properties").startObject("subj-type")
				.field("type", "keyword").field("index", "true")
				.field("copy_to", "cluster-id").endObject()
				.startObject("obj-type").field("type", "keyword")
				.field("index", "true").field("copy_to", "cluster-id")
				.endObject().startObject("relation").field("type", "keyword")
				.field("index", "true").field("copy_to", "cluster-id")
				.endObject().startObject("cluster-id").field("type", "keyword")
				.field("index", "true").endObject().startObject("tok-sent")
				.field("type", "text").endObject().startObject("page-id")
				.field("type", "integer").endObject().startObject("subj-pos")
				.field("type", "integer").endObject().startObject("obj-pos")
				.field("type", "integer").endObject().startObject("words")
				.startObject("properties").startObject("word")
				.field("type", "keyword").endObject().startObject("count")
				.field("type", "integer").endObject().endObject().endObject()
				.endObject() // properties
				.endObject() // documentType
				.endObject();

		App.LOG.debug("Mapping for cluster entry: " + mappingBuilder.string());
		PutMappingResponse putMappingResponse = indicesAdminClient
				.preparePutMapping(
						Config.getInstance().getString(
								Config.CLUSTER_ENTRY_INDEX))
				.setType(Config.getInstance().getString(Config.CLUSTER_ENTRY))
				.setSource(mappingBuilder).execute().actionGet();

		return putMappingResponse.isAcknowledged();
	}

	public boolean putMappingForTerms() throws IOException {
		IndicesAdminClient indicesAdminClient = getClient().admin().indices();
		XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
				.startObject()
				.startObject(Config.getInstance().getString(Config.TERM))
				.startObject("properties").startObject("term")
				.field("type", "text").endObject().startObject("tf")
				.field("type", "double").endObject().startObject("tf-idf")
				.field("type", "double").endObject().startObject("cluster-id")
				.field("type", "keyword").field("index", "true").endObject()
				.endObject() // properties
				.endObject()// documentType
				.endObject();

		App.LOG.debug("Mapping for terms: " + mappingBuilder.string());
		PutMappingResponse putMappingResponse = indicesAdminClient
				.preparePutMapping(
						Config.getInstance().getString(Config.TERM_INDEX))
				.setType(Config.getInstance().getString(Config.TERM))
				.setSource(mappingBuilder).execute().actionGet();
		return putMappingResponse.isAcknowledged();
	}

	public Entity getEntity(String entityId) {
		QueryBuilder query = QueryBuilders.termQuery("_id", entityId);
		// System.out.println(query);
		try {
			SearchRequestBuilder requestBuilder = getClient()
					.prepareSearch(
							Config.getInstance().getString(
									Config.WIKIDATA_INDEX))
					.setTypes(
							Config.getInstance().getString(
									Config.WIKIDATA_ENTITY))
					.storedFields("type", "label", "tok-label", "wiki-title",
							"aliases", "tok-aliases", "claims.property-id",
							"claims.object-id").setQuery(query).setSize(1);
			SearchResponse response = requestBuilder.execute().actionGet();
			// System.out.println(response);

			if (isResponseValid(response)) {
				for (SearchHit hit : response.getHits()) {
					return createEntity(hit);
				}
			}

		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

	public Entity createEntity(SearchHit hit) {
		String id = hit.getId();
		String type = hit.field("type").getValue().toString();
		String label = hit.field("label").getValue().toString();
		String tokLabel = hit.field("tok-label").getValue().toString();
		String wikiTitle = hit.field("wiki-title").getValue().toString();
		List<String> aliases = new ArrayList<String>();
		for (Object object : hit.field("aliases").getValues()) {
			aliases.add(String.valueOf(object));
		}
		List<String> tokAliases = new ArrayList<String>();
		for (Object object : hit.field("tok-aliases").getValues()) {
			tokAliases.add(String.valueOf(object));
		}
		List<Pair<String, String>> claims = new ArrayList<Pair<String, String>>();
		if (hit.field("claims.property-id") != null) {
			for (int i = 0; i < hit.field("claims.property-id").getValues()
					.size(); i++) {
				String propId = hit.field("claims.property-id").getValues()
						.get(i).toString();
				String objId = hit.field("claims.object-id").getValues().get(i)
						.toString();
				claims.add(new Pair<String, String>(propId, objId));
			}
		}
		Entity entity = new Entity(id, type, label, tokLabel, wikiTitle,
				aliases, tokAliases, claims);
		return entity;
	}

	public boolean isResponseValid(SearchResponse response) {
		return response != null && response.getHits().totalHits() > 0;
	}

	public List<Entity> getMultiEntities(List<String> idList) {

		List<Entity> itemList = new ArrayList<Entity>();
		MultiGetRequestBuilder requestBuilder = getClient().prepareMultiGet();
		for (String itemId : idList) {
			requestBuilder.add(new MultiGetRequest.Item(Config.getInstance()
					.getString(Config.WIKIDATA_INDEX), Config.getInstance()
					.getString(Config.WIKIDATA_ENTITY), itemId).storedFields(
					"type", "label", "tok-label", "wiki-title", "aliases",
					"tok-aliases", "claims.property-id", "claims.object-id"));
		}
		MultiGetResponse multiResponse = requestBuilder.execute().actionGet();
		for (MultiGetItemResponse multiGetItemResponse : multiResponse
				.getResponses()) {
			GetResponse response = multiGetItemResponse.getResponse();
			if (response.isExists()) {
				String id = response.getId();
				String type = response.getField("type").getValue().toString();
				String label = response.getField("label").getValue().toString();
				String tokLabel = response.getField("tok-label").getValue()
						.toString();
				String wikipediaTitle = response.getField("wiki-title")
						.getValue().toString();
				List<String> aliases = new ArrayList<String>();
				for (Object object : response.getField("aliases").getValues()) {
					aliases.add(String.valueOf(object));
				}
				List<String> tokAliases = new ArrayList<String>();
				for (Object object : response.getField("tok-aliases")
						.getValues()) {
					tokAliases.add(String.valueOf(object));
				}
				List<Pair<String, String>> claims = new ArrayList<Pair<String, String>>();
				if (response.getField("claims.property-id") != null) {
					for (int i = 0; i < response.getField("claims.property-id")
							.getValues().size(); i++) {
						String propId = response.getField("claims.property-id")
								.getValues().get(i).toString();
						String objId = response.getField("claims.object-id")
								.getValues().get(i).toString();
						claims.add(new Pair<String, String>(propId, objId));
					}
				}
				itemList.add(new Entity(id, type, label, tokLabel,
						wikipediaTitle, aliases, tokAliases, claims));
			}
		}
		return itemList;
	}

}
