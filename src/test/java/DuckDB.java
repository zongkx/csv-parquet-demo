import com.alibaba.fastjson2.JSON;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.duckdb.DuckDBConnection;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;

@Slf4j
public class DuckDB {
    private final Map<String, DuckDBWriter> DUCKDBWRITER_MAP = new ConcurrentHashMap<>();
    @Getter
    private final Queue<Object> DATA_QUEUE = new ConcurrentLinkedDeque<>();

    private final StampedLock stampLock = new StampedLock();

    private DuckDBConnection conn;

    @SneakyThrows
    public void init() {
        conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    List<Object> users = pollAll();
                    if (!users.isEmpty()) {
                        write(users.stream().collect(Collectors.groupingBy(a -> JSON.parseObject(JSON.toJSONString(a)).getLong("id"))));
                    }
                } catch (Exception e) {
                    log.error("批量消费异常", e);
                }
            }
        }, 0, 1000L);
    }

    public <T> void write(Map<Long, List<T>> collect) {
        collect.forEach((k, v) -> {
            String s = "/data/" + k;
            DuckDBWriter duckDBWriter = DUCKDBWRITER_MAP.computeIfAbsent(s, a -> new DuckDBWriter(s, conn));
            for (T t : v) {
                duckDBWriter.writeRow(t);
            }
        });
    }


    private List<Object> pollAll() {
        long stamp = stampLock.writeLock();
        try {
            List<Object> data = new ArrayList<>(this.DATA_QUEUE);
            this.DATA_QUEUE.clear();
            return data;
        } finally {
            stampLock.unlockWrite(stamp);
        }
    }

    public List<Map<String, Object>> queryForList(String sql) {
        try (DuckDBConnection duplicate = (DuckDBConnection) conn.duplicate();
             Statement stmt = duplicate.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<Map<String, Object>> result = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> rowMap = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    rowMap.put(columnName, columnValue);
                }
                result.add(rowMap);
            }
            return result;
        } catch (Exception e) {
            log.error("duckdb query error", e);
            return new ArrayList<>();
        }
    }

}
