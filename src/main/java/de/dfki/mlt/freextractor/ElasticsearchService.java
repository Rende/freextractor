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

import org.apache.flink.hadoop.shaded.com.google.common.collect.ImmutableList;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
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
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
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
			Map<String, String> userConfig = getUserConfig();
			List<InetSocketAddress> transportAddresses = getTransportAddresses();
			List<TransportAddress> transportNodes;
			transportNodes = new ArrayList<>(transportAddresses.size());
			for (InetSocketAddress address : transportAddresses) {
				transportNodes.add(new InetSocketTransportAddress(address));
			}
			Settings settings = Settings.settingsBuilder().put(userConfig)
					.build();

			TransportClient transportClient = TransportClient.builder()
					.settings(settings).build();
			for (TransportAddress transport : transportNodes) {
				transportClient.addTransportAddress(transport);
			}

			ImmutableList<DiscoveryNode> nodes = ImmutableList
					.copyOf(transportClient.connectedNodes());
			if (nodes.isEmpty()) {
				throw new RuntimeException(
						"Client is not connected to any Elasticsearch nodes!");
			}

			client = transportClient;
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
		if (indexExistReponse.isExists()) {
			deleteIndex(indicesAdminClient, indexName);
		}
		if (createIndex(indicesAdminClient, indexName)) {
			result = putMappingForRelations(indicesAdminClient);
		}
		return result;
	}

	private void deleteIndex(IndicesAdminClient indicesAdminClient,
			String indexName) {
		final DeleteIndexRequestBuilder delIdx = indicesAdminClient
				.prepareDelete(indexName);
		delIdx.execute().actionGet();
	}

	private boolean createIndex(IndicesAdminClient indicesAdminClient,
			String indexName) {
		final CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdminClient
				.prepareCreate(indexName).setSettings(
						Settings.settingsBuilder()
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

	private boolean putMappingForRelations(IndicesAdminClient indicesAdminClient)
			throws IOException {
		XContentBuilder mappingBuilder = XContentFactory
				.jsonBuilder()
				.startObject()
				.startObject(
						Config.getInstance().getString(
								Config.WIKIPEDIA_RELATION))
				.startObject("properties").startObject("page-id")
				.field("type", "integer").field("index", "not_analyzed")
				.endObject().startObject("subject-id").field("type", "string")
				.field("index", "not_analyzed").endObject()
				.startObject("subject-index").field("type", "integer")
				.field("index", "not_analyzed").endObject()
				.startObject("object-id").field("type", "string")
				.field("index", "not_analyzed").endObject()
				.startObject("object-index").field("type", "integer")
				.field("index", "not_analyzed").endObject()
				.startObject("surface").field("type", "string")
				.field("index", "not_analyzed").endObject()
				.startObject("start-index").field("type", "integer")
				.field("index", "not_analyzed").endObject()
				.startObject("end-index").field("type", "integer")
				.field("index", "not_analyzed").endObject()
				.startObject("property-id").field("type", "string")
				.field("index", "not_analyzed").endObject()
				.startObject("alias").field("type", "string").endObject()
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
					.addFields("type", "label", "wiki-title", "aliases",
							"claims.property_id", "claims.object-id")
					.setQuery(query).setSize(1);
			SearchResponse response = requestBuilder.execute().actionGet();
			// System.out.println(response);

			if (isResponseValid(response)) {
				for (SearchHit hit : response.getHits()) {
					String id = hit.getId();
					String type = hit.field("type").getValue().toString();
					String label = hit.field("label").getValue().toString();
					String wikiTitle = hit.field("wiki-title").getValue()
							.toString();
					List<String> aliases = new ArrayList<String>();
					for (Object object : hit.field("aliases").getValues()) {
						aliases.add(String.valueOf(object));
					}
					List<Pair<String, String>> claims = new ArrayList<Pair<String, String>>();
					if (hit.field("claims.property_id") != null) {
						for (int i = 0; i < hit.field("claims.property_id")
								.getValues().size(); i++) {
							String propId = hit.field("claims.property_id")
									.getValues().get(i).toString();
							String objId = hit.field("claims.object-id")
									.getValues().get(i).toString();
							claims.add(new Pair<String, String>(propId, objId));
						}
					}
					Entity entity = new Entity(id, type, label, wikiTitle,
							aliases, claims);
					return entity;
				}
			}

		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
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
					.getString(Config.WIKIDATA_ENTITY), itemId).fields("type",
					"label", "wiki-title", "aliases", "claims.property_id",
					"claims.object-id"));
		}
		MultiGetResponse multiResponse = requestBuilder.execute().actionGet();
		for (MultiGetItemResponse multiGetItemResponse : multiResponse
				.getResponses()) {
			GetResponse response = multiGetItemResponse.getResponse();
			if (response.isExists()) {
				String id = response.getId();
				String type = response.getField("type").getValue().toString();
				String label = response.getField("label").getValue().toString();
				String wikipediaTitle = response.getField("wiki-title")
						.getValue().toString();
				List<String> aliases = new ArrayList<String>();
				for (Object object : response.getField("aliases").getValues()) {
					aliases.add(String.valueOf(object));
				}
				List<Pair<String, String>> claims = new ArrayList<Pair<String, String>>();
				if (response.getField("claims.property_id") != null) {
					for (int i = 0; i < response.getField("claims.property_id")
							.getValues().size(); i++) {
						String propId = response.getField("claims.property_id")
								.getValues().get(i).toString();
						String objId = response.getField("claims.object-id")
								.getValues().get(i).toString();
						claims.add(new Pair<String, String>(propId, objId));
					}
				}
				itemList.add(new Entity(id, type, label, wikipediaTitle,
						aliases, claims));

			}
		}
		return itemList;
	}

}
