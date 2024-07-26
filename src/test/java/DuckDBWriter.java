import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.duckdb.DuckDBConnection;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.alibaba.fastjson.serializer.SerializerFeature.WriteMapNullValue;

@Slf4j
public class DuckDBWriter {
    public static final String COPY_TO_SQL = "copy (select * from read_json ('%s') )  to '%s' (format parquet, compression lz4_raw) ";
    private final String basePath;
    private final Path csvFile;
    private final AtomicInteger rowCounter = new AtomicInteger(0);
    private final DuckDBConnection duckDBConnection;
    private BufferedWriter currentWriter;

    public DuckDBWriter(String basePath, DuckDBConnection duckDBConnection) {
        this.basePath = basePath;
        this.duckDBConnection = duckDBConnection;
        this.csvFile = Paths.get(basePath + "/temp.jsonl");
        organize(true);
    }


    public void writeRow(Object t) {
        try {
            if (currentWriter == null) {
                currentWriter = createNewWriter();
            }
            currentWriter.write(JSONObject.toJSONString(t, WriteMapNullValue));
            currentWriter.newLine();
            currentWriter.flush();
            rowCounter.getAndIncrement();
            if (rowCounter.get() >= 10) {
                organize(false);
            }
        } catch (Exception e) {
            log.error("duckdb write error:", e);
        }
    }


    private void organize(Boolean t) {
        Stream<String> lines = null;
        try {
            if (t && !Files.exists(csvFile)) {//初始化且文件不存在直接返回
                return;
            }
            if (t && Files.exists(csvFile)) {
                lines = Files.lines(csvFile);
                if (!lines.findAny().isPresent()) {
                    return;
                }
            }
        } catch (Exception e) {
            log.error("duckdb merge error:", e);

        } finally {
            if (null != lines) {
                lines.close();
            }
        }
        Statement statement = null;
        try {
            if (currentWriter != null) {
                currentWriter.close();
            }
            statement = duckDBConnection.duplicate().createStatement();
            statement.execute(String.format(COPY_TO_SQL, csvFile, getParquetFileName()));
        } catch (Exception e) {
            log.error("duckdb merge error:", e);
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                rowCounter.set(0);
                currentWriter = createNewWriter();
            } catch (Exception e) {
                log.error("duckdb merge error:", e);
            }
        }
    }

    private BufferedWriter createNewWriter() throws IOException {
        Files.createDirectories(csvFile.getParent());
        return new BufferedWriter(new FileWriter(csvFile.toString()));
    }

    private String getParquetFileName() {
        return String.format("%s/%s.parquet", basePath, System.currentTimeMillis());
    }


}
