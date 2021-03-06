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

    // ?????????????????????
    @Test
    void testCreateIndex() throws IOException {
        // 1.??????????????????
        CreateIndexRequest request = new CreateIndexRequest("java_index");
        // 2.?????????????????????
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println("?????????????????????" + createIndexResponse.isAcknowledged() + "  ??????" + createIndexResponse.index());
    }

    // ????????????????????????
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("java_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println("?????????????????????" + exists);
    }

    // ?????????????????????
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("java_index");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println("??????????????????????????? " + delete.isAcknowledged());
    }

    // ?????????????????????
    @Test
    void testAddDocument() throws IOException {
        // 1.????????????
        User user = new User("?????????", 68);

        // 2.????????????
        IndexRequest request = new IndexRequest("java_index");
        // put /java_index/_doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1)); // ????????????request.timeout("1s");
        request.source(JSON.toJSONString(user), XContentType.JSON);

        // 3.???????????????????????????
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println("??????????????? " + indexResponse.status());
        System.out.println("??????????????? " + indexResponse.toString());
    }

    // ????????????????????????
    @Test
    void testIsExists() throws IOException {
        GetRequest getRequest = new GetRequest("java_index", "1");
        // ??????????????????_source???????????????
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println("?????????????????????" + exists);
    }

    // ?????????????????????
    @Test
    void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("java_index", "1");

        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println("???????????????" + getResponse.getSourceAsString());
        System.out.println("???????????????????????????" + getResponse);
    }

    // ?????????????????????
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("java_index", "1");
        updateRequest.timeout(TimeValue.timeValueSeconds(1));

        User user = new User("????????? ?????????", 70);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println("???????????????" + updateResponse.status());
        System.out.println("???????????????????????????" + updateResponse);
    }

    // ?????????????????????
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("java_index", "1");
        deleteRequest.timeout("1s");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println("?????????????????????"+ deleteResponse.status());
    }

    // ???????????? ????????????????????????????????????
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
        System.out.println("?????????????????????: "+bulkResponse.hasFailures());
    }

    // ??????????????????
    // ???????????????SearchRequest
    // ???????????????SearchSourceBuilder
    // ???????????????HighlightBuilder
    // ???????????????TermQueryBuilder
    // ???????????????MatchQueryBuilder

    @Test
    void testSearch() throws IOException {
        // ????????????
        SearchRequest searchRequest = new SearchRequest("java_index");
        // ????????????
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // ????????????
        // HighlightBuilder highlighter = searchSourceBuilder.highlighter();

        // ??????????????????????????????QueryBuilders???????????????
        // ????????????
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
