package com.chenqin.elasticsearch.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import javax.swing.text.rtf.RTFEditorKit;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.ngram.EdgeNGramFilterFactory;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chenqin.elasticsearch.util.EsConfigUtils;
import com.chenqin.elasticsearch.util.EsConstants;

public class EsClient {
	
	private static Logger logger = LoggerFactory.getLogger(EsClient.class);
	
	private static EsClient instance = null;
	
	private static TransportClient client = null;
	
	private static BulkProcessor bulkProcessor = null;

	public static EsClient getInstance(){
		if (instance == null) {
			instance = new EsClient();
		}
		return instance;
	}
	
	@SuppressWarnings({ "resource", "unchecked" })
	public static TransportClient getClient() {
		try {
			if (client == null) {
				Settings settings = Settings.builder()
						.put("cluster.name", "elasticsearch").build();
				PreBuiltTransportClient tClient = new PreBuiltTransportClient(
						settings);
				client = tClient
						.addTransportAddress(new InetSocketTransportAddress(
								InetAddress.getByName(EsConfigUtils.getIpAddress()),
								Integer.parseInt(EsConfigUtils.getPort())));
			}
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		return client;
	}
	
	
	public EsClient() {
		super();
		getClient();
	}

	/**
	 * 搜索数据
	 * @param queryBuilder
	 * @return
	 */
    public SearchResponse search(QueryBuilder queryBuilder){
         SearchResponse searchResponse = client.prepareSearch(EsConfigUtils.getIndexName()).setTypes(EsConfigUtils.getType()).setQuery(queryBuilder).execute().actionGet();
         return searchResponse;
    }
    
    /**
     * 插入数据
     * @param json
     * @param esId
     * @return
     */
    @SuppressWarnings("deprecation")
	public String insert(String json,String esId){
    	if (StringUtils.isEmpty(json) && StringUtils.isEmpty(esId)) {
    		logger.info("insert json or esId is null!");
			return "";
		}
    	IndexResponse indexResponse = client.prepareIndex(EsConfigUtils.getIndexName(),EsConfigUtils.getType(),esId)
		.setSource(json)
		.get();
    	logger.info("[insert]action:{},esId:{}",new Object[]{indexResponse.getResult().toString(),esId});
		return indexResponse.getResult().toString();
    }
    
    /**
     * 删除数据
     * @param esId
     * @return
     */
    public String delete(String esId){
    	if (StringUtils.isEmpty(esId)) {
    		logger.info("deleteIndex esId is null!");
			return "";
		}
    	DeleteResponse actionGet = client.prepareDelete(EsConfigUtils.getIndexName(),EsConfigUtils.getType(),esId).execute().actionGet();
    	logger.info("[delete]action:{},esId:{}",new Object[]{actionGet.getResult().toString(),esId});
		return actionGet.getResult().toString();
    }
    
    public static BulkProcessor getBulkProcessor(){
    	if (bulkProcessor == null) {
    		try {
				bulkProcessor = BulkProcessor.builder(getClient(), new BulkProcessor.Listener() {
					
					public void beforeBulk(long executionId, BulkRequest request) {
						// TODO Auto-generated method stub
						
					}
					
					public void afterBulk(long executionId, BulkRequest request,
							Throwable failure) {
						// TODO Auto-generated method stub
						logger.error( "insert fail!after failure=" + failure);
					}
					
					public void afterBulk(long executionId, BulkRequest request,
							BulkResponse response) {
						// TODO Auto-generated method stub
						logger.info("inset:{} doc,cost time {} ms,hasFailures :{}",new Object[]{response.getItems(),response.getTookInMillis(),response.hasFailures()});
					}
				})
				.setBulkActions(1000)//文档数量达到1000时提交
			    .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))//总文档体积达到5MB时提交 //
				.setFlushInterval(TimeValue.timeValueSeconds(5))//每5S提交一次（无论文档数量、体积是否达到阈值）
				.setConcurrentRequests(1)//加1后为可并行的提交请求数，即设为0代表只可1个请求并行，设为1为2个并行
				.build();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			
		}
		return bulkProcessor;
    }
    
    
    /** 
     * 创建mapping(feid("analyzer","ik_max_word")该字段分词IK索引 ；feid("search_analyzer","ik_max_word")该字段分词ik查询；具体分词插件请看IK分词插件说明) 
     * @param indices 索引名称； 
     * @param mappingType 类型 
     * @throws Exception 
     */  
    public  void createMapping(String indices,String mappingType)throws Exception{
    	if (StringUtils.isEmpty(indices) && StringUtils.isEmpty(mappingType)) {
    		logger.info("createMapping indices or mappingtype is null!");
			return;
		}
        client.admin().indices().prepareCreate(indices).execute().actionGet();  
        new XContentFactory();  
        XContentBuilder builder=XContentFactory.jsonBuilder()  
                .startObject()  
                .startObject(mappingType)  
                .startObject("properties")  
                .startObject("title").field("type", "string").field("store", "yes").field("analyzer","ik_max_word").field("search_analyzer","ik_max_word").endObject()  
                .startObject("company").field("type", "text").field("store", "yes").field("analyzer","ik_max_word").field("search_analyzer","ik_max_word").endObject()
                .startObject("name").field("type", "string").field("store", "yes").field("analyzer","ik_max_word").field("search_analyzer","ik_max_word").endObject()
                .startObject("meetingTags.tag_name").field("type", "string").field("store", "yes").field("analyzer","ik_max_word").field("search_analyzer","ik_max_word").endObject()
                .endObject()
                .endObject()
                .endObject();  
        PutMappingRequest mapping = Requests.putMappingRequest(indices).type(mappingType).source(builder);
//        System.out.println("======="+mapping.getShouldStoreResult());
        client.admin().indices().putMapping(mapping).actionGet();  
        client.close();  
    }
    
    public  boolean deleteIndex(String indexname){
    	if (StringUtils.isEmpty(indexname)) {
    	   logger.info("deleteIndex indexname is null!");
		   return false;
		}
    	DeleteIndexResponse actionGet = client.admin().indices().prepareDelete(indexname).execute().actionGet();
    	return actionGet.isAcknowledged();
    }
    
    
    public static void main(String[] args) throws Exception {
    	EsClient esHandler = new EsClient();
    	 //查询条件
//    	MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(Constants.MEETING_ID, "1246");
////    	FuzzyQueryBuilder fuzzyQuery = QueryBuilders.fuzzyQuery("company", "艺美酒业");
//    	MatchQueryBuilder matchQuery1 = QueryBuilders.matchQuery("company", "艺美");
////////    	WildcardQueryBuilder queryBuilder = QueryBuilders.wildcardQuery("name","*刚刚*");
////    	MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery(Constants.MEETING_TAGS_TAG_NAME, "政府人员");
//    	BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//    	boolQueryBuilder.must(matchQuery);
//    	boolQueryBuilder.must(matchQuery1);
////    	boolQueryBuilder.must(queryBuilder);
//////    	boolQueryBuilder.must(fuzzyQuery);
//    	SearchResponse search = esHandler.search(boolQueryBuilder);
//    	SearchHits hits = search.getHits();
//    	SearchHit[] searchHits = hits.getHits();
//    	System.out.println(searchHits.length);
//    	List<String> splitContent = splitContent("   政府人员    ");
//    	System.out.println(JSON.toJSONString(splitContent));
//    	System.out.println(checkContentEn("test"));
//    	String delete = esHandler.delete("0199b6b2-f8ff-4d94-9618-2ece8ee9dd28");
//    	System.out.println(delete);
    	
    	boolean deleteIndex = esHandler.deleteIndex("jackie");
//    	System.out.println(deleteIndex);
//    	esHandler.createMapping(EsConfigUtils.getIndexName(), EsConfigUtils.getType());
//    	esHandler.insert("{\"company\":\"艺美酒业@@@五粮液集团@@@贵州茅台集团\",\"createTime\":\"2017-11-01 17:28:12.0\",\"fakeUser\":false,\"id\":80113346,\"img\":\"https://g1test.jingwei.com/p/fmn101/img/20171020/05/54/Necx_jfg90554.jpg\",\"meetingId\":1246,\"meetingTags\":[{\"status\":0,\"tag_color\":\"FF9900\",\"tag_id\":10,\"tag_name\":\"采购商\"},{\"status\":0,\"tag_color\":\"66CC66\",\"tag_id\":12,\"tag_name\":\"供应商\"},{\"status\":0,\"tag_color\":\"CC6600\",\"tag_id\":16,\"tag_name\":\"观众\"},{\"status\":0,\"tag_color\":\"33CCFF\",\"tag_id\":4,\"tag_name\":\"政府人员\"}],\"name\":\"微信营销号\",\"status\":0,\"title\":\"销售经理兼任销售总监\",\"userId\":\"13999270\"}", String.valueOf(UUID.randomUUID()));
//    	MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("title", "销售经理");
//    	SearchResponse search = esHandler.search(matchQuery);
//    	SearchHit[] searchHits = search.getHits().getHits();
//    	if (searchHits.length > 0) {
//    		for (SearchHit searchHit : searchHits) {
//    			System.out.println(searchHit.getSource().get("company").toString());
//    		}
//		}
	}
}
