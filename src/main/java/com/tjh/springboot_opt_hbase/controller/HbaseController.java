package com.tjh.springboot_opt_hbase.controller;

import com.alibaba.fastjson.JSON;
import com.tjh.springboot_opt_hbase.common.constants.HbaseFlag;
import com.tjh.springboot_opt_hbase.common.constants.HbaseTableNameConstant;
import com.tjh.springboot_opt_hbase.common.templatelink.HbaseTemplateUtil;
import com.tjh.springboot_opt_hbase.model.vo.FilterCondition;
import com.tjh.springboot_opt_hbase.model.vo.FilterType;
import com.tjh.springboot_opt_hbase.model.vo.PageFilterVo;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/v1/test")
public class HbaseController {
    @Resource(name = HbaseFlag.OFFINE_HBASE)
    private HbaseTemplateUtil offineHbaseTemplate;

    @Resource(name = "columnToMapRowMapper")
    private RowMapper<Map<String, Object>> columnToMapRowMapper;

    @GetMapping(path = "/single_query")
    public String getHbase(@RequestParam String rowKey) {
        String result = "1";
        return result;
    }

    @PostMapping(path = "/batch_query")
    public String getBatchHbase(@RequestBody String rowKey) {
        String result = "";
        return result;
    }

    @GetMapping(path = "/add_single")
    public String addHbase(@RequestParam String rowKey, @RequestParam String data) {
        return null;
    }

    @PostMapping(path = "/add_batch")
    public String addBatchHbase(@RequestBody String batchData) {
        return null;
    }

    @GetMapping(path = "/query_row_filter")
    public String queryByRowFilter() {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("cscore"),
                CompareFilter.CompareOp.GREATER, Bytes.toBytes(2));
        List<FilterCondition> filterConditions=new ArrayList<>();
        FilterCondition filterCondition=new FilterCondition();
      /*  RowValueFilterVo rowValueFilter=new RowValueFilterVo();
        rowValueFilter.setData("2");
        rowValueFilter.setFamilyName("cf");
        rowValueFilter.setRowName("cscore");
        filterCondition.setFilter(rowValueFilter);*/
        filterCondition.setFilterType(FilterType.PAGE_FILTER);
        filterCondition.setOpt(CompareFilter.CompareOp.EQUAL);
        filterConditions.add(filterCondition);
        PageFilterVo pageFilterVo=new PageFilterVo();
        pageFilterVo.setPageSize(2);
        filterCondition.setFilter(pageFilterVo);
        List<Map<String, Object>> result = offineHbaseTemplate.getBatchByFilter(HbaseTableNameConstant.MODEL_DM_CSCORE_DOD3, filterConditions, columnToMapRowMapper);

        return JSON.toJSONString(result);
    }
}
