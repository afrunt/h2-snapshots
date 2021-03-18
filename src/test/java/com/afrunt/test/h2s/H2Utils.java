package com.afrunt.test.h2s;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * @author Andrii Frunt
 */
public class H2Utils {
    public static HikariDataSource createInMemoryDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:myDatabase;DB_CLOSE_DELAY=-1;LOG=0");

        return new HikariDataSource(config);
    }
}
