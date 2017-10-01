package socialite.async.dist.ds;

public enum MsgType {
    INIT_DATA,
    FEED_MY_IDX,
    FETCH_MY_IDX_WORKER_ID_MAP,

    REQUIRE_INIT_DATA,
    INIT_DATA_FEEDBACK,
    MESSAGE_TABLE, //发送buffer table给worker
    REQUIRE_TERM_CHECK,//Master require all works to compute partial value
    TERM_CHECK_PARTIAL_VALUE, //worker向master发送Δ之和
    TERM_CHECK_FEEDBACK,//master accumulate partial value to determine terminate or not.
}
