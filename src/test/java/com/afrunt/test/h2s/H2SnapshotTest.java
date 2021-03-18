package com.afrunt.test.h2s;

import com.afrunt.h2s.H2Snapshot;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/**
 * @author Andrii Frunt
 */
public class H2SnapshotTest {
    @Test
    public void testSnapshotCreation() {
        final H2Snapshot h2Snapshot = new H2Snapshot(H2Utils.createInMemoryDataSource());
        assertThat(h2Snapshot.getDirPath().toFile().exists()).isTrue();
        assertThat(h2Snapshot.getDumpFilePath().toFile().exists()).isTrue();
    }

    @Test
    public void testRestoreFromSnapshot() {
        final HikariDataSource dataSource = H2Utils.createInMemoryDataSource();
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
    public void testDeletedDumpFile(){
        final HikariDataSource dataSource = H2Utils.createInMemoryDataSource();
        final H2Snapshot initialStateSnapshot = new H2Snapshot(dataSource);
        final boolean dumpFileDeleted = initialStateSnapshot.getDumpFilePath().toFile().delete();
        assertThat(dumpFileDeleted).isTrue();
        assertThatThrownBy(initialStateSnapshot::getDumpFileContents).isExactlyInstanceOf(RuntimeException.class);
    }
}
