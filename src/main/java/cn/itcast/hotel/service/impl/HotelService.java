package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams params) {
        try {
            SearchRequest request = new SearchRequest("hotel");

            buildBasicQuery(params, request);

            request.source().from((params.getPage() - 1) * params.getSize()).size(params.getSize());

            String location = params.getLocation();
            if (location != null && !"".equals(location)) {
                request.source().sort(SortBuilders.geoDistanceSort("location", new GeoPoint(location))
                        .order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS));
            }

            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            return handleResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private PageResult handleResponse(SearchResponse response) {
        PageResult result = new PageResult();

        SearchHits searchHits = response.getHits();
        result.setTotal(searchHits.getTotalHits().value);

        SearchHit[] hits = searchHits.getHits();

        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length > 0) {
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotels.add(hotelDoc);
        }
        result.setHotels(hotels);

        return result;
    }

    private void buildBasicQuery(RequestParams params, SearchRequest request) {

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        if (params.getKey() == null || "".equals(params.getKey())) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", params.getKey()));
        }

        if (params.getCity() != null && !"".equals(params.getCity())) {
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }

        if (params.getBrand() != null && !"".equals(params.getBrand())) {
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }

        if (params.getStarName() != null && !"".equals(params.getStarName())) {
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }

        if (params.getMinPrice() != null && params.getMaxPrice() != null) {
            boolQuery.filter(QueryBuilders.rangeQuery("price")
                    .gte(params.getMinPrice())
                    .lte(params.getMaxPrice()));
        }

        if (!params.getSortBy().equals("default")) {
            request.source().sort(params.getSortBy(), SortOrder.DESC);
        }

        FunctionScoreQueryBuilder scoreQuery = QueryBuilders.functionScoreQuery(
                boolQuery,

                new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{

                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                QueryBuilders.termQuery("isAD", true),
                                ScoreFunctionBuilders.weightFactorFunction(10)
                        )
                });

        request.source().query(scoreQuery);
    }
}
