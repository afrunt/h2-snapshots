/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.afrunt.test.h2s;

import com.afrunt.h2s.H2Snapshot;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrii Frunt
 */
public class H2SnapshotTest {

    public static HikariDataSource createInMemoryDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:myDatabase;DB_CLOSE_DELAY=-1;LOG=0");

        return new HikariDataSource(config);
    }

    @Test
    public void testNullOriginalDataSource() {
        Assertions.assertThatThrownBy(() -> new H2Snapshot(null)).isExactlyInstanceOf(NullPointerException.class);
    }

    @Test
    public void testIncorrectTypeOfDataBase() throws SQLException {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getDatabaseProductName()).thenReturn("DEAD BEAF");

        Assertions.assertThatThrownBy(() -> new H2Snapshot(dataSource)).isExactlyInstanceOf(IllegalArgumentException.class);
    }


    @Test
    public void testSnapshotCreation() {
        final H2Snapshot h2Snapshot = new H2Snapshot(createInMemoryDataSource());
        assertThat(h2Snapshot.getDirPath().toFile().exists()).isTrue();
        assertThat(h2Snapshot.getDumpFilePath().toFile().exists()).isTrue();
    }

    @Test
    public void testRestoreFromSnapshot() {
        final HikariDataSource dataSource = createInMemoryDataSource();
        final H2Snapshot initialStateSnapshot = new H2Snapshot(dataSource);

        System.out.println(initialStateSnapshot.getDumpFileContents());

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        jdbcTemplate.execute("CREATE TABLE DUMMY( ID BIGINT, PRIMARY KEY (ID));");

        jdbcTemplate.execute("INSERT INTO DUMMY(ID) VALUES (1)");

        assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM DUMMY", Integer.class)).isEqualTo(1);

        final H2Snapshot updatedStateSnapshot = new H2Snapshot(dataSource);

        System.out.println(updatedStateSnapshot.getDumpFileContents());

        initialStateSnapshot.apply(dataSource);

        assertThatThrownBy(() -> jdbcTemplate.queryForObject("SELECT COUNT(*) FROM DUMMY", Integer.class)).isInstanceOf(Exception.class);

        updatedStateSnapshot.apply(dataSource);

        assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM DUMMY", Integer.class)).isEqualTo(1);
    }

    @Test
    public void testDeletedDumpFile() {
        final HikariDataSource dataSource = createInMemoryDataSource();
        final H2Snapshot initialStateSnapshot = new H2Snapshot(dataSource);
        final boolean dumpFileDeleted = initialStateSnapshot.getDumpFilePath().toFile().delete();
        assertThat(dumpFileDeleted).isTrue();
        assertThatThrownBy(initialStateSnapshot::getDumpFileContents).isExactlyInstanceOf(RuntimeException.class);
    }
}
