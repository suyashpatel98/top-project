package connection.pooling;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class QueryExecutor {

    private final JdbcTemplate jdbcTemplate;
    private final SchemaSelector schemaSelector;

    @Autowired
    public QueryExecutor(JdbcTemplate jdbcTemplate, SchemaSelector schemaSelector) {
        this.jdbcTemplate = jdbcTemplate;
        this.schemaSelector = schemaSelector;
    }

    public List<Map<String, Object>> executeQueryInSchema(String schemaName, String sql) {
        try {
            schemaSelector.useSchema(schemaName);
            return jdbcTemplate.queryForList(sql);
        } finally {
            schemaSelector.clearSchema();
        }
    }
}
