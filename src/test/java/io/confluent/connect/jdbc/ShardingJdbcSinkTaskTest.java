package io.confluent.connect.jdbc;

import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.JdbcSinkTask;
import io.confluent.connect.jdbc.source.EmbeddedDerby;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.shardingsphere.shardingjdbc.api.yaml.YamlShardingDataSourceFactory;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ShardingSphere Config Test
 *
 * @author gujian
 * @since 5.5.0
 **/
public class ShardingJdbcSinkTaskTest {

  private JdbcSinkTask task;
  private EmbeddedDerby db0;
  private EmbeddedDerby db1;
  private Map<String, String> connProps;

  private Logger log = LoggerFactory.getLogger(ShardingJdbcSinkTaskTest.class);

  @Mock
  private DatabaseDialect dialect;

  @Before
  public void setup() throws SQLException {
    task = new JdbcSinkTask();
    db0 = new EmbeddedDerby("db0");
    db1 = new EmbeddedDerby("db1");

    log.info(db0.getUrl());
    connProps = new HashMap<>();
    connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db0.getUrl());
    connProps.put(JdbcSinkConfig.TABLE_NAME_FORMAT, "t_order");

    IntStream.range(0, 21).forEach(idx -> {
      try {
        db0.createTable("t_order_"+idx,"trade_code", "VARCHAR(32)",  "user_id", "VARCHAR(32)", "store_id", "VARCHAR(32)", "status", "SMALLINT");
      } catch (SQLException throwables) {
        throwables.printStackTrace();
      }
    });
    IntStream.range(0, 21).forEach(idx -> {
      try {
        db1.createTable("t_order_"+idx,"trade_code", "VARCHAR(32)",  "user_id", "VARCHAR(32)", "store_id", "VARCHAR(32)", "status", "SMALLINT");
      } catch (SQLException throwables) {
        throwables.printStackTrace();
      }
    });
//    connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
//    connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
  }

  @After
  public void tearDown() throws Exception {
    db0.close();
    db0.dropDatabase();
    db1.close();
    db1.dropDatabase();
  }

  @Test
  public void testShardingSphereConfig() throws IOException {

    connProps.put(JdbcSourceConnectorConfig.SHARDING_SPHERE_YAML_CONF_CONFIG,
        getConfig());

    task.initialize(mock(SinkTaskContext.class));
    task.start(connProps);

    final Schema SCHEMA = SchemaBuilder.struct().name("com.gj.Order")
        .field("trade_code", Schema.STRING_SCHEMA)
        .field("user_id", Schema.STRING_SCHEMA)
        .field("store_id", Schema.STRING_SCHEMA)
        .field("status", Schema.INT32_SCHEMA).build();
    final Struct struct = new Struct(SCHEMA)
        .put("trade_code", "1212")
        .put("user_id", "223411")
        .put("store_id", "22414")
        .put("status", 0);

    SinkRecord recordA = new SinkRecord("atopic", 0, null, null, SCHEMA, struct, 0);

    task.put(Collections.singleton(recordA));
  }

  private String getConfig() throws IOException {
    return FileUtils.readFileToString(new File(
        this.getClass().getClassLoader().getResource("shardingSphere.yaml").getFile()));
  }

  @Test
  public void testShardingInsert() throws IOException, SQLException {
    DataSource dataSource = YamlShardingDataSourceFactory.createDataSource(getConfig().getBytes());
    Connection connection = dataSource.getConnection();
    String sql = "insert into \"t_order\"(\"trade_code\", \"user_id\", \"store_id\", \"status\") values(?, ?, ?, ?)";
    PreparedStatement statement = connection.prepareStatement(sql);
    statement.setString(1, "1");
    statement.setString(2,"2");
    statement.setString(3, "3");
    statement.setInt(4,0);

    int count = statement.executeUpdate();
    assertEquals(1, count);
  }
}
