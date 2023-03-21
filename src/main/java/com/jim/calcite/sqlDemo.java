package com.jim.calcite;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.ParserImpl;

/**
 * @author Jim Chen
 * @date 2022-05-31
 */
public class sqlDemo {

  public static void main(String[] args) throws SqlParseException {

    EnvironmentSettings settings = EnvironmentSettings
        .newInstance()
        .useBlinkPlanner()
        .inStreamingMode()
        .build();

    TableEnvironmentImpl tableEnv = TableEnvironmentImpl.create(settings);
    ParserImpl parser = (ParserImpl)tableEnv.getParser();

    // Sql语句
    String createTableSql = "create table hive_catalog.temp_rt.buf_mq_app_click_log_t_r_kafka (\n"
        + " `log`          row(file row(path string)) comment '日志路径'\n"
        + ",`message`      string comment '日志内容'\n"
        + ",`@timestamp`   string comment 'filebeat采集时间'\n"
        + ",`kafka_time`   TIMESTAMP(3) METADATA FROM 'timestamp' comment 'kafka记录的时间戳'\n"
        + ",`topic`        string metadata virtual comment 'kafka topic'\n"
        + ",`partition`    bigint metadata virtual comment 'kafka partition'\n"
        + ",`offset`       bigint metadata virtual comment 'kafka offset'\n"
        + ") with (\n"
        + " 'connector' = 'kafka'\n"
        + ",'topic' = 'bigdata_mobile50bang_logs'\n"
        + ",'properties.bootstrap.servers' = 'wx12-test-hadoop003:6667'\n"
        + ",'properties.group.id' = 'bg_datadev'\n"
        + ",'scan.startup.mode' = 'group-offsets'\n"
        + ",'format' = 'json'\n"
        + ")";

    String createViewSql = "create temporary view tv_1\n"
        + "as\n"
        + "select\n"
        + " *\n"
        + ",substring(p_ts,12,2) as event_time\n"
        + "from hive_catalog.dw_db.retains_kafka /*+ OPTIONS('connector.properties.group.id'='ads_adh_prd_active_dev_1h_rt') */\n"
        + "where is_active='1'\n"
        + "\n";

    String createViewSql2 = "create temporary view tv_2\n"
        + "as\n"
        + "select\n"
        + " *\n"
        + ",substring(p_ts,12,2) as event_time\n"
        + "from hive_catalog.dw_db.retains_kafka \n"
        + "where is_active='1'\n"
        + "\n";

    String insertSql = "insert into hive_catalog.buf.buf_mq_app_click_log_t_r\n"
        + "select\n"
        + " split_index(message,'\\t',0) as ip\n"
        + ",split_index(message,'\\t',1) as `json`\n"
        + ",split_index(message,'\\t',2) as `server_time`\n"
        + ",split_index(log.file.path ,',',1) as project\n"
        + ",`@timestamp` as filebeat_time\n"
        + ",substring(cast(kafka_time as string), 1, 19) as kafka_time\n"
        + ",convert_tz(cast(proctime() as string), 'utc', 'asia/shanghai') as flink_time\n"
        + ",concat_ws('|', topic, cast(`partition` as string), cast(`offset` as string)) as sys_guid\n"
        + ",split_index(log.file.path ,',',2) as p_dt\n"
        + ",split_index(split_index(log.file.path ,',',3) ,'_',0) as p_hr\n"
        + "from hive_catalog.temp_rt.buf_mq_app_click_log_t_r_kafka /*+ options('properties.group.id'='buf_mq_app_click_log_t_r') */\n"
        + "";

    String insertSql2 = "insert into hive_catalog.buf.buf_mq_app_click_log_t_r\n"
        + "select\n"
        + " split_index(message,'\\t',0) as ip\n"
        + ",split_index(message,'\\t',1) as `json`\n"
        + ",split_index(message,'\\t',2) as `server_time`\n"
        + ",split_index(log.file.path ,',',1) as project\n"
        + ",`@timestamp` as filebeat_time\n"
        + ",substring(cast(kafka_time as string), 1, 19) as kafka_time\n"
        + ",convert_tz(cast(proctime() as string), 'utc', 'asia/shanghai') as flink_time\n"
        + ",concat_ws('|', topic, cast(`partition` as string), cast(`offset` as string)) as sys_guid\n"
        + ",split_index(log.file.path ,',',2) as p_dt\n"
        + ",split_index(split_index(log.file.path ,',',3) ,'_',0) as p_hr\n"
        + "from hive_catalog.temp_rt.buf_mq_app_click_log_t_r_kafka \n"
        + "";

    String insertSql3 = "insert into `dwd`.`user` select * from t1";

    SqlNode insert = parser.parseSqlNode(insertSql);
    SqlNode insert2 = parser.parseSqlNode(insertSql2);
    SqlNode insert3 = parser.parseSqlNode(insertSql3);

    SqlNode createTable = parser.parseSqlNode(createTableSql);
    SqlNode createView = parser.parseSqlNode(createViewSql);
    SqlNode createView2 = parser.parseSqlNode(createViewSql2);

    if (insert instanceof RichSqlInsert) {
      processRichSqlInsert((RichSqlInsert) insert);
    }
    if (insert2 instanceof RichSqlInsert) {
      processRichSqlInsert((RichSqlInsert) insert2);
    }
    if (insert3 instanceof RichSqlInsert) {
      processRichSqlInsert((RichSqlInsert) insert3);
    }
    if (createTable instanceof SqlCreateTable) {
      processSqlCreateTable((SqlCreateTable) createTable);
    }
    if (createView instanceof SqlCreateView) {
      processSqlCreateView((SqlCreateView) createView);
    }
    if (createView2 instanceof SqlCreateView) {
      processSqlCreateView((SqlCreateView) createView2);
    }

  }

  private static void processSqlCreateView(SqlCreateView createView) {
    String viewName = createView.getViewName().toString();
    boolean temporary = createView.isTemporary();
    SqlNode query = createView.getQuery();

    SqlNode from = ((SqlSelect) query).getFrom();

    String sourceTable = null;
    Map<String, String> optionKVPairs = new HashMap<>();

    if ("TABLE_REF".equals(from.getKind().toString())) {
      // 有option
      List<SqlNode> operandList = ((SqlTableRef) from).getOperandList();
      sourceTable = operandList.get(0).toString();

      SqlNode sourceTableOptions = operandList.get(1);
      List<SqlNode> sourceTableOptionsList = ((SqlNodeList) sourceTableOptions).getList();
      if (sourceTableOptionsList != null && sourceTableOptionsList.size() > 0) {
        for (SqlNode sqlNode : sourceTableOptionsList) {
          optionKVPairs.putAll(((SqlHint)sqlNode).getOptionKVPairs());
        }
      }
    } else if ("IDENTIFIER".equals(from.getKind().toString())) {
      sourceTable = from.toString();
    }

    System.out.println(sourceTable);
    System.out.println(optionKVPairs);

  }

  private static void processSqlCreateTable(SqlCreateTable createTable) {
    String tableName = createTable.getTableName().toString();
    System.out.println(tableName);

    Map<String, String> withOptions = new HashMap<>();
    SqlNodeList propertyList = createTable.getPropertyList();
    List<SqlNode> list = propertyList.getList();
    for (SqlNode sqlNode : list) {
      withOptions.put(
          ((SqlTableOption) sqlNode).getKeyString(),
          ((SqlTableOption) sqlNode).getValueString());
    }
    System.out.println(withOptions);

  }

  private static void processRichSqlInsert(RichSqlInsert parsed) {
    RichSqlInsert insert = parsed;

    // source
    SqlNode from = ((SqlSelect) insert.getSource()).getFrom();

    String sourceTable = null;
    Map<String, String> optionKVPairs = new HashMap<>();

    List<SqlNode> operandList;
    if ("TABLE_REF".equals(from.getKind().toString())) {
      operandList = ((SqlTableRef) from).getOperandList();
      sourceTable = operandList.get(0).toString();
      if (operandList.size() > 0) {
        SqlNode sourceTableOptions = operandList.get(1);
        List<SqlNode> sourceTableOptionsList = ((SqlNodeList) sourceTableOptions).getList();
        if (sourceTableOptionsList != null && sourceTableOptionsList.size() > 0) {
          for (SqlNode sqlNode : sourceTableOptionsList) {
            optionKVPairs.putAll(((SqlHint) sqlNode).getOptionKVPairs());
          }
        }
      }
    } else if ("IDENTIFIER".equals(from.getKind().toString())) {
      sourceTable = from.toString();
    }
    System.out.println(sourceTable);
    System.out.println(optionKVPairs);

    // target
    String targetTable = insert.getTargetTable().toString();
    System.out.println(targetTable);
  }


}
