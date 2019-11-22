package com.tjh.springboot_opt_hbase.common.templatelink;


import com.tjh.springboot_opt_hbase.model.vo.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


public class HbaseTemplateUtil extends HbaseTemplate {
    //https://www.cnblogs.com/hello-daocaoren/p/9605209.html
    public HbaseTemplateUtil(Configuration configuration) {
        super(configuration);
    }

    /**
     * 某个列族下添加多列
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnValues
     */
    public int putMulRow(String tableName, String rowKey, String familyName, Map<String, Object> columnValues) {
        int result = this.execute(tableName, new TableCallback<Integer>() {
            @Override
            public Integer doInTable(HTableInterface hTableInterface) throws Throwable {
                List<Put> puts = columnValues.entrySet().stream().filter(e -> {
                    return e.getKey() != null && e.getValue() != null;
                }).map(e -> {
                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(e.getKey()), Bytes.toBytes(Objects.toString(e.getValue())));
                    return put;
                }).collect(Collectors.toList());

                hTableInterface.put(puts);
                return puts.size();
            }
        });

        this.execute(tableName, new TableCallback<Object>() {
            @Override
            public Object doInTable(HTableInterface hTableInterface) throws Throwable {
                return null;
            }
        });

        return result;
    }

    /**
     * 批量插入多行数据
     *
     * @param tableName
     * @param familyName
     * @param rowName
     * @param rowsData
     * @param <T>
     * @return
     */
    public <T> int putMulLine(String tableName, String familyName, String rowName, List<KeyValueModel<T>> rowsData) {
        int result = this.execute(tableName, new TableCallback<Integer>() {
            @Override
            public Integer doInTable(HTableInterface hTableInterface) throws Throwable {
                List<Put> puts = rowsData.stream().filter(e -> {
                    return e.getKey() != null && e.getValue() != null;
                }).map(e -> {
                    Put put = new Put(Bytes.toBytes(e.getKey()));
                    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(rowName), Bytes.toBytes(Objects.toString(e.getValue())));
                    return put;
                }).collect(Collectors.toList());

                hTableInterface.put(puts);
                return puts.size();
            }
        });

        return result;
    }

    /**
     * 获取指定rowkey下的某几列名称的数据
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param rowNames
     * @param rowMapper
     * @param <T>
     * @return
     */
    public <T> T getSingleRowkeyMulRow(String tableName, String rowKey, String familyName, String[] rowNames, RowMapper<T> rowMapper) {
        T rs = this.execute(tableName, new TableCallback<T>() {
            @Override
            public T doInTable(HTableInterface hTableInterface) throws Throwable {
                Get get = new Get(Bytes.toBytes(rowKey));
                if (rowNames != null) {
                    for (String rowName : rowNames) {
                        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(rowName));
                    }
                }

                Result result = hTableInterface.get(get);
                return rowMapper.mapRow(result, 0);
            }
        });

        return rs;
    }

    /**
     * 批量获取rowkey的多列数据
     *
     * @param tableName
     * @param rowKeyList
     * @param familyName
     * @param rowNames
     * @param rowMapper
     * @param <T>
     * @return
     */
    public <T> List<T> getBatchRowkeyMulRow(String tableName, List<String> rowKeyList, String familyName, String[] rowNames, RowMapper<T> rowMapper) {
        List<T> rs = this.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(HTableInterface hTableInterface) throws Throwable {
                List<Get> gets = rowKeyList.stream().filter(e -> {
                    return !StringUtils.isEmpty(e);
                }).map(e -> {
                    Get get = new Get(Bytes.toBytes(e));
                    if (rowNames == null) {
                        //获取所有列
                        return get;
                    }
                    for (String rowName : rowNames) {
                        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(rowName));
                    }

                    return get;
                }).collect(Collectors.toList());

                Result[] results = hTableInterface.get(gets);
                List<T> data = new ArrayList<>();
                if (results != null && results.length > 0) {
                    for (Result result : results) {
                        data.add(rowMapper.mapRow(result, 0));
                    }
                }
                return data;
            }
        });

        return rs;
    }

    /**
     * 删除一个rowkey的数据
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param rowNameList
     * @return
     */
    public int delete(String tableName, String rowKey, String familyName, List<String> rowNameList) {
        int rs = this.execute(tableName, new TableCallback<Integer>() {
            @Override
            public Integer doInTable(HTableInterface hTableInterface) throws Throwable {
                Delete del = new Delete(Bytes.toBytes(rowKey));
                if (!StringUtils.isEmpty(familyName)) {
                    del.addFamily(Bytes.toBytes(familyName));
                }

                if (rowNameList != null) {
                    for (String rowName : rowNameList) {
                        del.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(rowName));
                    }
                }

                hTableInterface.delete(del);
                return del.size();
            }
        });

        return rs;
    }

    /**
     * 删除多个rowkey的数据
     *
     * @param tableName
     * @param rowKeyList
     * @return
     */
    public int deleteBatch(String tableName, List<String> rowKeyList) {
        int rs = this.execute(tableName, new TableCallback<Integer>() {
            @Override
            public Integer doInTable(HTableInterface hTableInterface) throws Throwable {
                List<Delete> deleteList = new ArrayList<>();
                for (String key : rowKeyList) {
                    Delete del = new Delete(Bytes.toBytes(key));
                    deleteList.add(del);
                }

                hTableInterface.delete(deleteList);
                return deleteList.size();
            }
        });

        return rs;
    }

    /**
     * 批量筛选
     *
     * @param tableName
     * @param filterConditions
     * @param rowMapper
     * @param <T>
     * @return
     */
    public <T> List<T> getBatchByFilter(String tableName, List<FilterCondition> filterConditions, RowMapper<T> rowMapper) {
        List<T> rs = this.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(HTableInterface hTableInterface) throws Throwable {
                Scan scan = new Scan();
              /* 列值过滤 SingleColumnValueFilter filter =  new SingleColumnValueFilter( Bytes.toBytes("testfm"),  Bytes.toBytes("name"),
                        CompareOperator.EQUAL,  Bytes.toBytes("wd")) ;*/
                /* 列名前缀过滤  ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("name"));*/
                /* rowkey过滤 RowFilter filter = new RowFilter(CompareOperator.EQUAL,new RegexStringComparator("^hgs_00*"));*/
                /*  RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator("^hgs_00*"));*/
                FilterList filterList = new FilterList();
                if (filterConditions != null) {
                    for (FilterCondition filterCondition : filterConditions) {
                        if (FilterType.KEY_FILTER == filterCondition.getFilterType()) {
                            RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(filterCondition.getFilter().toString()));
                            filterList.addFilter(filter);
                        } else if (FilterType.ROW_FILTER == filterCondition.getFilterType()) {
                            RowValueFilterVo rowValueFilter = (RowValueFilterVo) filterCondition.getFilter();
                            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(rowValueFilter.getFamilyName()), Bytes.toBytes(rowValueFilter.getRowName()),
                                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes(rowValueFilter.getData()));
                            filterList.addFilter(filter);
                            // scan.addColumn(Bytes.toBytes(filterCondition.getFamilyName()),Bytes.toBytes(filterCondition.getRowName()));
                        } else if (FilterType.ROW_PREFIX == filterCondition.getFilterType()) {
                            ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(filterCondition.getFilter().toString()));
                            filterList.addFilter(filter);
                        }
                        else if(FilterType.PAGE_FILTER == filterCondition.getFilterType())
                        {
                            //主要用于现在每次取出的数据条数
                            PageFilterVo  pageFilterVo=(PageFilterVo)filterCondition.getFilter();
                            Filter filter = new PageFilter(pageFilterVo.getPageSize());
                            filterList.addFilter(filter);
                        }
                    }
                }

                scan.setFilter(filterList);
                ResultScanner resultScanner = hTableInterface.getScanner(scan);
                List<T> data = new ArrayList<>();
                for (Result rs : resultScanner) {
                    data.add(rowMapper.mapRow(rs, 0));
                }

                return data;
            }
        });

        return rs;
    }
}
