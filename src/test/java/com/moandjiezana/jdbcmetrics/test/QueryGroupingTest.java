package com.moandjiezana.jdbcmetrics.test;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import com.soulgalore.jdbcmetrics.JDBCMetrics;

public class QueryGroupingTest {

  private Connection connection;

  @Before
  public void before() throws SQLException {
    connection = DriverManager.getConnection("jdbc:jdbcmetrics:h2:mem:");
  }

  @Test
  public void should_group_by_query() throws Exception {
    PreparedStatement statement = connection.prepareStatement("SELECT 2");
    statement.execute();
    statement.execute();

    connection.prepareStatement("SELECT 1").execute();
    Map<String, Metric> metrics = new HashMap<String, Metric>(JDBCMetrics.getInstance().getRegistry().getMetrics());

    Iterator<Entry<String, Metric>> iterator = metrics.entrySet().iterator();

    while (iterator.hasNext()) {
      Entry<String, Metric> entry = iterator.next();

      if (!entry.getKey().startsWith("jdbc.query.")) {
        iterator.remove();
      }
    }

    assertThat(metrics.keySet(), hasSize(2));
    assertEquals(2, ((Timer) metrics.get("jdbc.query.SELECT 2")).getCount());
    assertEquals(1, ((Timer) metrics.get("jdbc.query.SELECT 1")).getCount());
  }

  @After
  public void after() throws SQLException {
    connection.close();
  }
}
