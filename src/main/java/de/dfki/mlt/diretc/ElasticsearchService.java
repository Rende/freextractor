/**
 *
 */
package de.dfki.mlt.diretc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequestValidationException;
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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
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

import de.dfki.mlt.diretc.preferences.Config;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class ElasticsearchService {
	private Client client;

	public ElasticsearchService() {
		initClient();
	}

	private void initClient() {
		try {
			getClient();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("resource")
	public Client getClient() throws UnknownHostException {
		if (client == null) {
			Settings settings = Settings.builder()
					.put(Config.CLUSTER_NAME, Config.getInstance().getString(Config.CLUSTER_NAME)).build();
			client = new PreBuiltTransportClient(settings).addTransportAddress(
					new TransportAddress(InetAddress.getByName(Config.getInstance().getString(Config.HOST)),
							Config.getInstance().getInt(Config.PORT)));
		}
		return client;
	}

	public List<HttpHost> getHosts() {
		List<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost(Config.getInstance().getString(Config.HOST),
				Config.getInstance().getInt(Config.HTTP_PORT), "http"));
		return httpHosts;
	}

	public void stopConnection() throws UnknownHostException {
		getClient().close();
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
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();
		{
			// builder.field("dynamic", "true");
			builder.startObject("properties");
			{
				builder.startObject("subj-type");
				{
					builder.field("type", "keyword");
				}
				builder.endObject();
				builder.startObject("obj-type");
				{
					builder.field("type", "keyword");
				}
				builder.endObject();
				builder.startObject("relation");
				{
					builder.field("type", "keyword");
				}
				builder.endObject();
				builder.startObject("cluster-id");
				{
					builder.field("type", "keyword");
				}
				builder.endObject();
				builder.startObject("tok-sent");
				{
					builder.field("type", "text");
				}
				builder.endObject();
				builder.startObject("page-id");
				{
					builder.field("type", "integer");
				}
				builder.endObject();
				builder.startObject("subj-pos");
				{
					builder.field("type", "integer");
				}
				builder.endObject();
				builder.startObject("obj-pos");
				{
					builder.field("type", "integer");
				}
				builder.endObject();
				builder.startObject("words");
				{
					builder.field("type", "nested");
					builder.startObject("properties");
					{
						builder.startObject("word");
						{
							builder.field("type", "keyword");
						}
						builder.endObject();
						builder.startObject("count");
						{
							builder.field("type", "integer");
						}
						builder.endObject();
					}
					builder.endObject();
				}
				builder.endObject();
			}
			builder.endObject();
		}
		builder.endObject();
		System.out.println(builder.toString());
		App.LOG.debug("Mapping for type cluster member: " + builder.toString());
		AcknowledgedResponse putMappingResponse = indicesAdminClient
				.preparePutMapping(Config.getInstance().getString(Config.TYPE_CLUSTER_INDEX))
				.setType(Config.getInstance().getString(Config.CLUSTER_MEMBER)).setSource(builder).execute()
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

		App.LOG.debug("Mapping for terms: " + mappingBuilder.toString());
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
					Entity entity = createEntity(hit.getSourceAsMap());
					entity.setId(hit.getId());
					return entity;
				}
			}

		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private Entity createEntity(Map<String, Object> dataMap) {
		String type = dataMap.get("type").toString();
		String datatype = dataMap.get("datatype").toString();
		HashMap<String, String> labels = (HashMap<String, String>) dataMap.get("labels");
		HashMap<String, String> lemLabels = (HashMap<String, String>) dataMap.get("lem-labels");
		HashMap<String, String> descriptions = (HashMap<String, String>) dataMap.get("descriptions");
		HashMap<String, String> lemDescriptions = (HashMap<String, String>) dataMap.get("lem-descriptions");
		HashMap<String, List<String>> aliases = (HashMap<String, List<String>>) dataMap.get("aliases");
		HashMap<String, List<String>> lemAliases = (HashMap<String, List<String>>) dataMap.get("lem-aliases");
		List<HashMap<String, String>> claims = (ArrayList<HashMap<String, String>>) dataMap.get("claims");
		Entity entity = new Entity(type, datatype, labels, lemLabels, descriptions, lemDescriptions, aliases,
				lemAliases, claims);
		return entity;
	}

	private boolean isResponseValid(SearchResponse response) {
		return response != null && response.getHits().getTotalHits() > 0;
	}

	public List<Entity> getMultiEntities(List<String> idList) throws UnknownHostException {
		List<Entity> itemList = new ArrayList<Entity>();
		MultiGetRequestBuilder requestBuilder = getClient().prepareMultiGet();
		for (String itemId : idList) {
			requestBuilder.add(new MultiGetRequest.Item(Config.getInstance().getString(Config.WIKIDATA_INDEX),
					Config.getInstance().getString(Config.WIKIDATA_ENTITY), itemId));
		}
		try {
			MultiGetResponse multiResponse = requestBuilder.execute().actionGet();
			for (MultiGetItemResponse multiGetItemResponse : multiResponse.getResponses()) {
				GetResponse response = multiGetItemResponse.getResponse();
				if (response != null && response.isExists()) {
					Entity entity = createEntity(response.getSource());
					entity.setId(response.getId());
					itemList.add(entity);
				}
			}
		} catch (ActionRequestValidationException e) {
			App.LOG.error("Subject not found: " + idList.get(0));
			e.printStackTrace();
		}
		return itemList;
	}

	@SuppressWarnings("unchecked")
	public Collection<Terms.Bucket> getClusters() throws UnknownHostException {
		SearchResponse response = getClient().prepareSearch(Config.getInstance().getString(Config.TYPE_CLUSTER_INDEX))
				.setTypes(Config.getInstance().getString(Config.CLUSTER_MEMBER)).setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.terms("clusters").field("cluster-id").size(Integer.MAX_VALUE))
				.setFetchSource(true).setExplain(false).execute().actionGet();

		Terms terms = response.getAggregations().get("clusters");
		Collection<Terms.Bucket> buckets = (Collection<Bucket>) terms.getBuckets();
		return buckets;
	}

	public long getClusterNumber() throws UnknownHostException {
		int clusterCount = 0;
		for (Bucket bucket : getClusters()) {
			if (bucket.getDocCount() >= Config.getInstance().getInt(Config.MIN_CLUSTER_SIZE)) {
				clusterCount++;
			}
		}
		return clusterCount;
	}

	public SearchResponse getClusterEntryHits(String clusterId) throws UnknownHostException {
		SearchRequestBuilder builder = getClient()
				.prepareSearch(Config.getInstance().getString(Config.TYPE_CLUSTER_INDEX))
				.setScroll(new TimeValue(60000)).setTypes(Config.getInstance().getString(Config.CLUSTER_MEMBER))
				.setQuery(QueryBuilders.matchQuery("cluster-id", clusterId));
		System.out.println(builder.toString());
		SearchResponse response = builder.setSize(Config.getInstance().getInt(Config.SCROLL_SIZE)).execute()
				.actionGet();
		return response;

	}

}
