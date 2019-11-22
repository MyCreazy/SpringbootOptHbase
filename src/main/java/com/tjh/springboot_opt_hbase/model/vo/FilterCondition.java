package com.tjh.springboot_opt_hbase.model.vo;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class FilterCondition<T> {
    private T filter;
    /*   EQUAL                                  相等
       GREATER                              大于
       GREATER_OR_EQUAL           大于等于
       LESS                                      小于
       LESS_OR_EQUAL                  小于等于
       NOT_EQUAL                        不等于*/
    private CompareFilter.CompareOp opt;
    private FilterType filterType;

    public FilterType getFilterType() {
        return filterType;
    }

    public void setFilterType(FilterType filterType) {
        this.filterType = filterType;
    }


    public CompareFilter.CompareOp getOpt() {
        return opt;
    }

    public void setOpt(CompareFilter.CompareOp opt) {
        this.opt = opt;
    }

    public T getFilter() {
        return filter;
    }

    public void setFilter(T filter) {
        this.filter = filter;
    }
}
