package connection.pooling;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class MultiSchemaQueryDemoApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(MultiSchemaQueryDemoApplication.class, args);

        MultiThreadedQueryExecutor executor = context.getBean(MultiThreadedQueryExecutor.class);

        // List of schemas to query
        List<String> schemas = Arrays.asList("schema1", "schema2");

        // SQL query to execute
        String sql = "SELECT * FROM users LIMIT 5";

        // Execute the query across all schemas
        executor.executeQueriesInParallel(schemas, sql);

        // Close the application context
        context.close();
    }
}
