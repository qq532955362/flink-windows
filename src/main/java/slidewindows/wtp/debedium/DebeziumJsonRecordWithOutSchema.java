package slidewindows.wtp.debedium;

import lombok.Data;

@Data
public class DebeziumJsonRecordWithOutSchema {

    private Source source;
    private Object before;
    private Object after;
    private String op;
    private Long ts_ms;
    private String transaction;

    @Data
    public static class Source {
        private String version;
        private String connector;
        private String name;
        private Long ts_ms;
        private String snapshot;
        private String db;
        private String sequence;
        private String table;
        private Integer server_id;
        private Long gtid;
        private String file;
        private Long pos;
        private Long row;
        private String thread;
        private String query;
    }

}
