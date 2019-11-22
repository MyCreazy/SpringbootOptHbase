package com.tjh.springboot_opt_hbase.common.config;

import com.tjh.springboot_opt_hbase.common.templatelink.HbaseTemplateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@org.springframework.context.annotation.Configuration
public class HbaseConfig {
    @Value("${hbase.offline.zookeeper.quorum}")
    private String offineZookeeperQueue;

    @Bean("offineHbaseConfig")
    @Primary
    public Configuration offineConfiguration()
    {
        Configuration conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",this.offineZookeeperQueue);
        conf.set("zookeeper.session.timeout","1200000");
        return conf;
    }

    @Bean("offlineHbaseTemplate")
    @Primary
    public HbaseTemplateUtil offlineHbaseTemplate(@Qualifier("offineHbaseConfig") Configuration configuration) {
        HbaseTemplateUtil rs = new HbaseTemplateUtil(configuration);
        rs.setAutoFlush(false);
        return rs;
    }
}
