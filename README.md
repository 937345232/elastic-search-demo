## Meeting-search  ##
### 项目作用  ###
- 在`elasticsearch`的接口进行封装，用于微信公众号项目`bcc-weixin`参会人员的搜索；  
### 配置  ###
- 配置文件`elasticseachsetting.properties`中进行配置`elasticsearch`搭建的服务器IP地址，端口号，index和type；  

### 对外接口 ###
接口名|作用|其他
-------|--------|-----
 public SearchResponse search(QueryBuilder queryBuilder) | 搜索接口 | 
public String insert(String json,String esId) | 插入数据 | 
public String delete(String esId) | 根据elasticsearch Id 删除某条数据 | 
public  void createMapping(String indices,String mappingType) | 创建需要进行分词的数据 | 
public  boolean deleteIndex(String indexname) | 根据索引index删除该索引下全部的数据 | **该接口慎用**，会导致数据被删除



