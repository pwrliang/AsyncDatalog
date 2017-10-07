package socialite.async.dist.ds;

public enum MsgType {
    NOTIFY_INIT,
    MESSAGE_TABLE, //发送buffer table给worker
    REQUIRE_TERM_CHECK,//Master require all works to compute partial value
    TERM_CHECK_PARTIAL_VALUE, //worker向master发送Δ之和
    TERM_CHECK_FEEDBACK,//master accumulate partial value to determine terminate or not.
}
