package ru.n1ppl3.thrift.kafka.jdbc.saver.repository;

import lombok.AllArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.UserLogsRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.abbreviate;
import static org.apache.commons.lang3.StringUtils.isEmpty;


@Repository
public class UserLogsRepository {

    private static final String INSERT_STATEMENT = "INSERT INTO USER_LOGS (OPER, REMOTEADDR, COMMENTS, SUCCESS, PRS_ID, DATE_IN, ALLADDR, UA, SID, OBJECT_TYPE, OBJECT_ID, MSG_ID) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPSERT_STATEMENT = "MERGE INTO USER_LOGS USING dual ON (MSG_ID=?) " +
            "WHEN NOT MATCHED THEN INSERT (OPER, REMOTEADDR, COMMENTS, SUCCESS, PRS_ID, DATE_IN, ALLADDR, UA, SID, OBJECT_TYPE, OBJECT_ID, MSG_ID) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";


    private final JdbcTemplate jdbcTemplate;

    public UserLogsRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }


    /**
     *
     */
    public Long getRecordsCount() {
        return jdbcTemplate.queryForObject("select count(id) from USER_LOGS", Long.class);
    }

    /**
     *
     */
    public int saveOne(UserLogsRecord r) {
        return jdbcTemplate.update(INSERT_STATEMENT, r.getOper(), r.getRemoteAddr(), r.getComment(), r.isSuccess(),
                r.getPrsId(), new Timestamp(r.getDateIn().toEpochMilli()),
                r.getAllAddr(), r.getUa(), r.getSid(), r.getObjectType(), r.getObjectId(), r.getMessageId());
    }

    /**
     *
     */
    public int saveOneMerged(UserLogsRecord r) {
        return jdbcTemplate.update(UPSERT_STATEMENT, r.getMessageId(), r.getOper(), r.getRemoteAddr(), r.getComment(), r.isSuccess(),
                r.getPrsId(), new Timestamp(r.getDateIn().toEpochMilli()),
                r.getAllAddr(), r.getUa(), r.getSid(), r.getObjectType(), r.getObjectId(), r.getMessageId());
    }

    /**
     *
     */
    public int saveMany(List<UserLogsRecord> records) {
        int[] result = jdbcTemplate.batchUpdate(INSERT_STATEMENT, new AbstractBatchPreparedStatementSetter(records) {
            @Override
            public void setValues(PreparedStatement stmt, int i) throws SQLException {
                setStatement(stmt, records.get(i), 0);
            }
        });
        return sum(result);
    }

    /**
     *
     */
    public int saveManyNative(List<UserLogsRecord> records) {
        int[] result = jdbcTemplate.execute(con -> con.prepareStatement(INSERT_STATEMENT), (PreparedStatementCallback<int[]>) ps -> {
            for (UserLogsRecord record : records) {
                setStatement(ps, record, 0);
                ps.addBatch();
            }
            return ps.executeBatch();
        });
        assert result != null;
        return sum(result);
    }

    /**
     *
     */
    public int saveManyMerged(List<UserLogsRecord> records) {
        int[] result = jdbcTemplate.batchUpdate(UPSERT_STATEMENT, new AbstractBatchPreparedStatementSetter(records) {
            @Override
            public void setValues(PreparedStatement stmt, int i) throws SQLException {
                UserLogsRecord record = records.get(i);
                stmt.setString(1, record.getMessageId());
                setStatement(stmt, record, 1);
            }
        });
        return sum(result);
    }

    /**
     *
     */
    public int saveManyMergedNative(List<UserLogsRecord> records) {
        int[] result = jdbcTemplate.execute(con -> con.prepareStatement(UPSERT_STATEMENT), (PreparedStatementCallback<int[]>) ps -> {
            for (UserLogsRecord record : records) {
                ps.setString(1, record.getMessageId());
                setStatement(ps, record, 1);
                ps.addBatch();
            }
            return ps.executeBatch();
        });
        assert result != null;
        return sum(result);
    }


    private static void setStatement(PreparedStatement stmt, UserLogsRecord record, int j) throws SQLException {
        String comment = isEmpty(record.getComment()) ? "[empty]" : abbreviate(record.getComment(), 1000);

        stmt.setInt(j + 1, record.getOper());
        stmt.setString(j + 2, abbreviate(record.getRemoteAddr(), 17));
        stmt.setString(j + 3, comment);
        stmt.setBoolean(j + 4, record.isSuccess());
        stmt.setLong(j + 5, record.getPrsId());
        stmt.setTimestamp(j + 6, new Timestamp(record.getDateIn().toEpochMilli()));
        stmt.setString(j + 7, abbreviate(record.getAllAddr(), 50));
        stmt.setString(j + 8, abbreviate(record.getUa(), 4000));
        stmt.setString(j + 9, abbreviate(record.getSid(), 100));
        stmt.setString(j + 10, abbreviate(record.getObjectType(), 30));
        stmt.setString(j + 11, abbreviate(record.getObjectId(), 40));
        stmt.setString(j + 12, record.getMessageId());
    }


    /**
     *
     */
    public List<UserLogsRecord> findAllByRemoteAddr(String remoteAddr) {
        String sql =
            "SELECT OPER, REMOTEADDR, COMMENTS, SUCCESS, PRS_ID, DATE_IN, ALLADDR, UA, SID, OBJECT_TYPE, OBJECT_ID, MSG_ID FROM USER_LOGS WHERE REMOTEADDR = ? ORDER BY ID";
        RowMapper<UserLogsRecord> rowMapper = (rs, rowNum) -> new UserLogsRecord()
                .setOper(rs.getInt(1))
                .setRemoteAddr(rs.getString(2))
                .setComment(rs.getString(3))
                .setSuccess(rs.getInt(4) != 0)
                .setPrsId(rs.getLong(5))
                .setDateIn(rs.getTimestamp(6).toInstant())
                .setAllAddr(rs.getString(7))
                .setUa(rs.getString(8))
                .setSid(rs.getString(9))
                .setObjectType(rs.getString(10))
                .setObjectId(rs.getString(11))
                .setMessageId(rs.getString(12));
        return jdbcTemplate.query(sql, rowMapper, remoteAddr);
    }


    private static int sum(int[] ints) {
        int result = 0;
        for (int num : ints) {
            // https://coderanch.com/t/303802/databases/JDBC-batch-update-return - объяснение минусов
            result += (num == -2) ? 1 : num;
        }
        return result;
    }


    @AllArgsConstructor
    abstract static class AbstractBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

        protected final List<UserLogsRecord> records;

        @Override
        public int getBatchSize() {
            return records.size();
        }
    }

}
