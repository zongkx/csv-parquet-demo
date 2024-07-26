/**
 * @author zongkx
 */

import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * @author zongkx
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ATests {
    DuckDB duckDB;

    @BeforeAll
    public void before() {
        duckDB = new DuckDB();
        duckDB.init();

    }

    @Test
    @SneakyThrows
    void a2() {
        List<Map<String, Object>> maps = duckDB.queryForList("select * from 'C:\\data\\0\\*.parquet'");
        System.out.println(maps);
    }

    @Test
    @SneakyThrows
    void a1() {
        List<CompletableFuture<Void>> threadList = new ArrayList<>(100);
        for (int i = 0; i < 1; i++) {
            int finalI = i;
            threadList.add(CompletableFuture.runAsync(() -> {
                for (int j = 0; j < 111; j++) {
                    duckDB.getDATA_QUEUE().add(new User((long) finalI, "demo" + j));
                    try {
                        Thread.sleep(10L);
                    } catch (Exception e) {
                    }
                }
            }));
        }
        if (!threadList.isEmpty()) {
            CompletableFuture.allOf(threadList.toArray(new CompletableFuture[0])).join();// join 等待结果,不join 则不等待
        }
        Thread.sleep(1000000L);
    }
}