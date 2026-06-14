package nexus;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * نموذج رسالة الدردشة — مع Vector Clock للترتيب السببي
 * يجاوب سؤال الامتحان: Message Schema (ID, Sender, Receiver, Timestamp, Content, Status)
 */
public class ChatMessage implements Serializable {

    public enum Status { SENT, DELIVERED, READ }
    public enum FanoutType { WRITE_TIME, READ_TIME }

    public final String    id;
    public final String    sender;
    public final String    receiver;   // null = broadcast
    public final String    content;
    public final long      timestamp;
    public volatile Status status;
    public final int[]     vectorClock; // للترتيب السببي

    public ChatMessage(String sender, String receiver, String content, int[] vc) {
        this.id          = UUID.randomUUID().toString().substring(0, 8);
        this.sender      = sender;
        this.receiver    = receiver;
        this.content     = content;
        this.timestamp   = Instant.now().toEpochMilli();
        this.status      = Status.SENT;
        this.vectorClock = vc != null ? vc.clone() : new int[]{0, 0, 0};
    }

    public String toJson() {
        return "{\"id\":\"" + id + "\",\"sender\":\"" + esc(sender) + "\",\"receiver\":\""
                + esc(receiver == null ? "all" : receiver) + "\",\"content\":\""
                + esc(content) + "\",\"timestamp\":" + timestamp + ",\"status\":\""
                + status.name() + "\",\"vc\":[" + vectorClock[0] + ","
                + vectorClock[1] + "," + vectorClock[2] + "]}";
    }

    private static String esc(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    @Override public String toString() {
        return "[" + sender + "→" + (receiver==null?"all":receiver) + "] " + content + " (" + status + ")";
    }

}
