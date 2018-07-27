/**
 *
 */
package de.dfki.mlt.freextractor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

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

	@SuppressWarnings("resource")
	public Client getClient() {
		if (client == null) {
			Settings settings = Settings.builder()
					.put(Config.CLUSTER_NAME, Config.getInstance().getString(Config.CLUSTER_NAME)).build();
			try {
				client = new PreBuiltTransportClient(settings).addTransportAddress(
						new InetSocketTransportAddress(InetAddress.getByName("134.96.187.233"), 9300));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		return client;

	}

	public static Map<String, String> getUserConfig() {
		Map<String, String> config = new HashMap<>();
		config.put(Config.BULK_FLUSH_MAX_ACTIONS, Config.getInstance().getString(Config.BULK_FLUSH_MAX_ACTIONS));
		config.put(Config.CLUSTER_NAME, Config.getInstance().getString(Config.CLUSTER_NAME));

		return config;
	}

	public static List<InetSocketAddress> getTransportAddresses() {
		List<InetSocketAddress> transports = new ArrayList<>();
		try {
			transports.add(new InetSocketAddress(InetAddress.getByName(Config.getInstance().getString(Config.HOST)),
					Config.getInstance().getInt(Config.PORT)));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return transports;
	}

	public boolean checkAndCreateIndex(String indexName) throws IOException, InterruptedException {
		boolean result = false;
		IndicesAdminClient indicesAdminClient = getClient().admin().indices();
		final IndicesExistsResponse indexExistReponse = indicesAdminClient.prepareExists(indexName).execute()
				.actionGet();
		if (indexExistReponse.isExists()) {
			deleteIndex(indicesAdminClient, indexName);
		}
		result = createIndex(indicesAdminClient, indexName);
		return result;
	}

	private void deleteIndex(IndicesAdminClient indicesAdminClient, String indexName) {
		final DeleteIndexRequestBuilder delIdx = indicesAdminClient.prepareDelete(indexName);
		delIdx.execute().actionGet();
	}

	private boolean createIndex(IndicesAdminClient indicesAdminClient, String indexName) {
		final CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdminClient.prepareCreate(indexName)
				.setSettings(Settings.builder()
						.put(Config.NUMBER_OF_SHARDS, Config.getInstance().getInt(Config.NUMBER_OF_SHARDS))
						.put(Config.NUMBER_OF_REPLICAS, Config.getInstance().getInt(Config.NUMBER_OF_REPLICAS)));
		CreateIndexResponse createIndexResponse = null;
		createIndexResponse = createIndexRequestBuilder.execute().actionGet();
		return createIndexResponse != null && createIndexResponse.isAcknowledged();
	}

	public boolean putMappingForClusterEntry() throws IOException {
		IndicesAdminClient indicesAdminClient = getClient().admin().indices();
		XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().startObject()
				.startObject(Config.getInstance().getString(Config.CLUSTER_ENTRY)).startObject("properties")
				.startObject("subj-type").field("type", "keyword").field("index", "true").endObject()
				.startObject("obj-type").field("type", "keyword").field("index", "true").endObject()
				.startObject("relation").field("type", "keyword").field("index", "true").endObject()
				.startObject("is-cluster-member").field("type", "boolean").endObject().startObject("relation-id")
				.field("type", "keyword").endObject().startObject("cluster-id").field("type", "keyword")
				.field("index", "true").endObject().startObject("subj-name").field("type", "keyword").endObject()
				.startObject("obj-name").field("type", "keyword").endObject().startObject("relation-phrase")
				.field("type", "keyword").endObject().startObject("tok-sent").field("type", "text").endObject()
				.startObject("page-id").field("type", "integer").endObject().startObject("subj-pos")
				.field("type", "integer").endObject().startObject("obj-pos").field("type", "integer").endObject()
				.startObject("words").startObject("properties").startObject("word").field("type", "keyword").endObject()
				.startObject("count").field("type", "integer").endObject().endObject().endObject().endObject() // properties
				.endObject() // documentType
				.endObject();

		App.LOG.debug("Mapping for cluster entry: " + mappingBuilder.string());
		PutMappingResponse putMappingResponse = indicesAdminClient
				.preparePutMapping(Config.getInstance().getString(Config.CLUSTER_ENTRY_INDEX))
				.setType(Config.getInstance().getString(Config.CLUSTER_ENTRY)).setSource(mappingBuilder).execute()
				.actionGet();

		return putMappingResponse.isAcknowledged();
	}

	public boolean putMappingForTerms() throws IOException {
		IndicesAdminClient indicesAdminClient = getClient().admin().indices();
		XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().startObject()
				.startObject(Config.getInstance().getString(Config.TERM)).startObject("properties").startObject("term")
				.field("type", "keyword").field("index", "true").endObject().startObject("tf").field("type", "float")
				.endObject().startObject("tf-idf").field("type", "float").endObject().startObject("cluster-id")
				.field("type", "keyword").field("index", "true").endObject().endObject() // properties
				.endObject()// documentType
				.endObject();

		App.LOG.debug("Mapping for terms: " + mappingBuilder.string());
		PutMappingResponse putMappingResponse = indicesAdminClient
				.preparePutMapping(Config.getInstance().getString(Config.TERM_INDEX))
				.setType(Config.getInstance().getString(Config.TERM)).setSource(mappingBuilder).execute().actionGet();
		return putMappingResponse.isAcknowledged();
	}

	public Entity getEntity(String entityId) {
		QueryBuilder query = QueryBuilders.termQuery("_id", entityId);
		try {
			SearchRequestBuilder requestBuilder = getClient()
					.prepareSearch(Config.getInstance().getString(Config.WIKIDATA_INDEX))
					.setTypes(Config.getInstance().getString(Config.WIKIDATA_ENTITY)).setQuery(query).setSize(1);
			SearchResponse response = requestBuilder.execute().actionGet();

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

	@SuppressWarnings("unchecked")
	private Entity createEntity(SearchHit hit) {
		String id = hit.getId();
		String type = hit.getSource().get("type").toString();
		String label = hit.getSource().get("label").toString();
		String tokLabel = hit.getSource().get("tok-label").toString();
		String wikiTitle = "";
		if (hit.getSource().containsKey("wiki-title"))
			wikiTitle = hit.getSource().get("wiki-title").toString();
		List<String> aliases = (ArrayList<String>) hit.getSource().get("aliases");
		List<String> tokAliases = (ArrayList<String>) hit.getSource().get("tok-aliases");
		List<HashMap<String, String>> claims = (ArrayList<HashMap<String, String>>) hit.getSource().get("claims");
		Entity entity = new Entity(id, type, label, tokLabel, wikiTitle, aliases, tokAliases, claims);
		return entity;
	}

	private boolean isResponseValid(SearchResponse response) {
		return response != null && response.getHits().totalHits() > 0;
	}

	@SuppressWarnings("unchecked")
	public List<Entity> getMultiEntities(List<String> idList) {

		List<Entity> itemList = new ArrayList<Entity>();
		MultiGetRequestBuilder requestBuilder = getClient().prepareMultiGet();
		for (String itemId : idList) {
			requestBuilder.add(new MultiGetRequest.Item(Config.getInstance().getString(Config.WIKIDATA_INDEX),
					Config.getInstance().getString(Config.WIKIDATA_ENTITY), itemId));
		}
		MultiGetResponse multiResponse = requestBuilder.execute().actionGet();
		for (MultiGetItemResponse multiGetItemResponse : multiResponse.getResponses()) {
			GetResponse response = multiGetItemResponse.getResponse();
			// System.out.println("getMultiEntities response: " + response);
			if (response.isExists()) {
				String id = response.getId();
				String type = response.getSource().get("type").toString();
				String label = response.getSource().get("label").toString();
				String tokLabel = response.getSource().get("tok-label").toString();
				String wikipediaTitle = "";
				if (response.getSource().containsKey("wiki-title"))
					wikipediaTitle = response.getSource().get("wiki-title").toString();
				List<String> aliases = (ArrayList<String>) response.getSource().get("aliases");
				List<String> tokAliases = (ArrayList<String>) response.getSource().get("tok-aliases");
				List<HashMap<String, String>> claims = (ArrayList<HashMap<String, String>>) response.getSource()
						.get("claims");
				itemList.add(new Entity(id, type, label, tokLabel, wikipediaTitle, aliases, tokAliases, claims));
			}
		}
		return itemList;
	}

	public Collection<Terms.Bucket> getClusters() {
		SearchResponse response = App.esService.getClient()
				.prepareSearch(Config.getInstance().getString(Config.CLUSTER_ENTRY_INDEX))
				.setTypes(Config.getInstance().getString(Config.CLUSTER_ENTRY)).setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.terms("clusters").field("cluster-id").size(Integer.MAX_VALUE))
				.setFetchSource(true).setExplain(false).execute().actionGet();

		Terms terms = response.getAggregations().get("clusters");
		Collection<Terms.Bucket> buckets = terms.getBuckets();
		return buckets;
	}

	public long getClusterNumber() {
		int clusterCount = 0;
		for (Bucket bucket : getClusters()) {
			if (bucket.getDocCount() >= Config.getInstance().getInt(Config.MIN_CLUSTER_SIZE)) {
				clusterCount++;
			}
		}
		return clusterCount;
	}

	public SearchResponse getClusterEntryHits(String clusterId) {
		SearchRequestBuilder builder = App.esService.getClient()
				.prepareSearch(Config.getInstance().getString(Config.CLUSTER_ENTRY_INDEX))
				.setScroll(new TimeValue(60000)).setTypes(Config.getInstance().getString(Config.CLUSTER_ENTRY))
				.setQuery(QueryBuilders.matchQuery("cluster-id", clusterId));
		System.out.println(builder.toString());
		SearchResponse response = builder.setSize(Config.getInstance().getInt(Config.SCROLL_SIZE)).execute()
				.actionGet();
		return response;

	}

}
