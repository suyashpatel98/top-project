package connection.pooling;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class MultiThreadedQueryExecutor {

    private final QueryExecutor queryExecutor;

    @Autowired
    public MultiThreadedQueryExecutor(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    public void executeQueriesInParallel(List<String> schemaNames, String sql) {
        List<Thread> threads = new ArrayList<>();
        ConcurrentHashMap<String, List<Map<String, Object>>> results = new ConcurrentHashMap<>();

        for (String schemaName : schemaNames) {
            Thread thread = new Thread(() -> {
                try {
                    List<Map<String, Object>> queryResult = queryExecutor.executeQueryInSchema(schemaName, sql);
                    results.put(schemaName, queryResult);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Print out the results
        for (String schemaName : schemaNames) {
            System.out.println("Results for schema: " + schemaName);
            List<Map<String, Object>> schemaResults = results.get(schemaName);
            if (schemaResults != null && !schemaResults.isEmpty()) {
                for (Map<String, Object> row : schemaResults) {
                    System.out.println(row);
                }
            } else {
                System.out.println("No results or query didn't return any data.");
            }
            System.out.println();  // Empty line for readability
        }
    }
}
