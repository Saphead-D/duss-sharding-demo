package com.duss;

import com.alibaba.druid.pool.DruidDataSource;
import io.shardingsphere.api.config.ShardingRuleConfiguration;
import io.shardingsphere.api.config.TableRuleConfiguration;
import io.shardingsphere.api.config.strategy.InlineShardingStrategyConfiguration;
import io.shardingsphere.shardingjdbc.api.ShardingDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Sharding {
    public static DataSource createDataSource(String username, String password, String url){
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setUrl(url);
        return dataSource;
    }

    public static void execute(DataSource dataSource, String sql, int oid,int uid,String name) throws SQLException {
        Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setInt(1, oid);
        statement.setInt(2, uid);
        statement.setString(3, name);
        statement.execute();
    }

    public static void main(String[] args) throws SQLException {
        Map<String, DataSource> map = new HashMap<>();
        map.put("kkb_ds_0", createDataSource("root", "root", "jdbc:mysql://192.168.142.128:3306/kkb_ds_0"));
        map.put("kkb_ds_1", createDataSource("root", "root", "jdbc:mysql://192.168.142.128:3306/kkb_ds_1"));

        ShardingRuleConfiguration configuration = new ShardingRuleConfiguration();

        // 配置Order表规则
        TableRuleConfiguration orderConfig = new TableRuleConfiguration();
        orderConfig.setLogicTable("t_order");//设置逻辑表
        orderConfig.setActualDataNodes("kkb_ds_${0..1}.t_order_${0..1}");//设置实际数据节点
        orderConfig.setKeyGeneratorColumnName("oid");//设置主键列名称
        // 配置Order表规则：配置分库 + 分表策略(这个也可以在ShardingRuleConfiguration进行统一设置)
        orderConfig.setDatabaseShardingStrategyConfig(new InlineShardingStrategyConfiguration("uid", "kkb_ds_${uid % 2}"));
        orderConfig.setTableShardingStrategyConfig(new InlineShardingStrategyConfiguration("oid", "t_order_${oid % 2}"));
        configuration.getTableRuleConfigs().add(orderConfig);

        DataSource dataSource = ShardingDataSourceFactory.createDataSource(map, configuration, new HashMap<>(), new Properties());
        for(int i=1;i<=10;i++) {
            String sql="insert into t_order(oid,uid,name) values(?,?,?)";
            execute(dataSource,sql,i,i,i+"aaa");
        }
        System.out.println("数据插入完成。。。");
    }
}
