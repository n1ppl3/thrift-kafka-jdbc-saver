package ru.n1ppl3.thrift.kafka.jdbc.saver.entity;

import lombok.Data;
import lombok.experimental.Accessors;

import java.time.Instant;

import static ru.n1ppl3.thrift.kafka.jdbc.saver.utils.Utils.sysGuid;

@Data
@Accessors(chain = true)
public class UserLogsRecord {

    private int oper; // required
    private String remoteAddr;
    private String comment; // required
    private boolean success; // required
    private long prsId; // required
    private Instant dateIn; // required
    private String allAddr;
    private String ua;
    private String sid;
    private String objectType;
    private String objectId;
    private String messageId = sysGuid();

    public UserLogsRecord withNewMessageId() {
        return setMessageId(sysGuid());
    }

    public UserLogsRecord deepCopy() {
        return copyOf(this);
    }

    public static UserLogsRecord copyOf(UserLogsRecord origin) {
        return new UserLogsRecord()
                .setOper(origin.oper)
                .setRemoteAddr(origin.remoteAddr)
                .setComment(origin.comment)
                .setSuccess(origin.success)
                .setPrsId(origin.prsId)
                .setDateIn(origin.dateIn)
                .setAllAddr(origin.allAddr)
                .setUa(origin.ua)
                .setSid(origin.sid)
                .setObjectType(origin.objectType)
                .setObjectId(origin.objectId)
                .setMessageId(origin.messageId);
    }

}
