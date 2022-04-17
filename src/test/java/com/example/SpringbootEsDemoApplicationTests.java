package com.example;

import com.alibaba.fastjson.JSON;
import com.example.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class SpringbootEsDemoApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    @Test
    void contextLoads() {
    }

    // 测试索引的创建
    @Test
    void testCreateIndex() throws IOException {
        // 1.创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("java_index");
        // 2.客户端执行请求
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println("是否创建成功：" + createIndexResponse.isAcknowledged() + "  索引" + createIndexResponse.index());
    }

    // 测试索引是否存在
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("java_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println("索引是否存在：" + exists);
    }

    // 测试索引的删除
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("java_index");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println("索引是否删除成功： " + delete.isAcknowledged());
    }

    // 测试文档的创建
    @Test
    void testAddDocument() throws IOException {
        // 1.创建对象
        User user = new User("特朗普", 68);

        // 2.创建请求
        IndexRequest request = new IndexRequest("java_index");
        // put /java_index/_doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1)); // 也可以用request.timeout("1s");
        request.source(JSON.toJSONString(user), XContentType.JSON);

        // 3.发送请求，获取结果
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println("响应状态： " + indexResponse.status());
        System.out.println("响应结果： " + indexResponse.toString());
    }

    // 测试文档是否存在
    @Test
    void testIsExists() throws IOException {
        GetRequest getRequest = new GetRequest("java_index", "1");
        // 不获取返回的_source的上下文了
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println("文档是否存在：" + exists);
    }

    // 测试文档的获取
    @Test
    void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("java_index", "1");

        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println("获取文档：" + getResponse.getSourceAsString());
        System.out.println("获取文档详细信息：" + getResponse);
    }

    // 测试文档的更新
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("java_index", "1");
        updateRequest.timeout(TimeValue.timeValueSeconds(1));

        User user = new User("唐纳德 特朗普", 70);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println("更新状态：" + updateResponse.status());
        System.out.println("获取文档详细信息：" + updateResponse);
    }

    // 测试文档的删除
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("java_index", "1");
        deleteRequest.timeout("1s");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println("文档是否删除："+ deleteResponse.status());
    }

    // 特殊的， 真的项目会批量插入数据！
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        List<User> userList = new ArrayList<>();
        userList.add(new User("Trump1", 66));
        userList.add(new User("Trump2", 67));
        userList.add(new User("Trump3", 68));
        userList.add(new User("Biden1", 68));
        userList.add(new User("Biden2", 69));
        userList.add(new User("Biden3", 70));

        for (int i = 0; i < userList.size(); i++) {
            bulkRequest.add(
                new IndexRequest("java_index")
                    .id(""+(i+1))
                    .source(JSON.toJSONString(userList.get(i)), XContentType.JSON)
            );
        }

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println("批处理是否失败: "+bulkResponse.hasFailures());
    }

    // 查询（搜索）
    // 搜索请求：SearchRequest
    // 条件构造：SearchSourceBuilder
    // 构建高亮：HighlightBuilder
    // 精确查询：TermQueryBuilder
    // 模糊查询：MatchQueryBuilder

    @Test
    void testSearch() throws IOException {
        // 搜索请求
        SearchRequest searchRequest = new SearchRequest("java_index");
        // 条件构造
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 构建高亮
        // HighlightBuilder highlighter = searchSourceBuilder.highlighter();

        // 查询条件，我们可以用QueryBuilders工具来实现
        // 精确查询
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name.keyword", "Trump1");
        searchSourceBuilder.query(termQueryBuilder);

        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        System.out.println("==============");
        for (SearchHit documentFields : searchResponse.getHits().getHits()) {
            System.out.println(documentFields.getSourceAsMap());
        }
    }
}
