package com.tjh.springboot_opt_hbase.common.templatelink;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component("columnToMapRowMapper")
public class ColumnToMapRowMapper implements RowMapper<Map<String,Object>>{

    @Override
    public Map<String, Object> mapRow(Result result, int rowNumber) throws Exception {
        if (result == null || result.isEmpty()) {
            return null;
        }
        Map<String, Object> rs = new HashMap<>();
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                    cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            rs.put(column, value);
        }
        return rs;
    }
}
