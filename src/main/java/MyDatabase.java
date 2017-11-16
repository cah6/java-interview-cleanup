import java.util.Map;

public interface MyDatabase {

    /**
     * Database expects a document of the form:
     * {
     *     id: 1,
     *     version: 2,
     *     isoDates: [ 2008-09-15T15:53:00, 2017-08-15T15:53:00 ],
     *     truncatedDates: [ 1484524800000, 1508371200000 ],
     *     metadata: {
     *         accountName: customer1,
     *         eventType: biz_txn_v1,
     *         retentionPeriod: 10,
     *         pickupTimestamp: 1510858845481
     *     },
     *     pickupSpeed: "WITHIN_HOUR"
     * }
     *
     * retentionPeriod is an optional field.
     */
    void indexDocument(Map<?, ?> doc);
}
