package com.afrunt.test.h2s;

import com.afrunt.h2s.H2DataSourceValidator;
import com.zaxxer.hikari.HikariDataSource;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static com.afrunt.test.h2s.H2Utils.createInMemoryDataSource;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrii Frunt
 */
public class H2DataSourceValidatorTest {

    @Test
    public void testNullOriginalDataSource() {
        Assertions.assertThatThrownBy(() -> new H2DataSourceValidator().validate(null)).isExactlyInstanceOf(NullPointerException.class);
    }


    @Test
    public void testDataSourceTypeValidation() {
        try (HikariDataSource inMemoryDataSource = createInMemoryDataSource()) {
            new H2DataSourceValidator().validate(inMemoryDataSource);
        }
        // Success
    }

    @Test
    public void testIncorrectTypeOfDataBase() throws SQLException {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getDatabaseProductName()).thenReturn("DEAD BEAF");

        Assertions.assertThatThrownBy(() -> new H2DataSourceValidator().validate(dataSource)).isExactlyInstanceOf(IllegalArgumentException.class);
    }
}
