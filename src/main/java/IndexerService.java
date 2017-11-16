import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class takes some json, transforms it, and gives it to the database. This code is NOT performance intensive --
 * we want to focus on readability over micro-optimizations.
 */
public class IndexerService {

    private MyDatabase myDatabase;

    public IndexerService(MyDatabase myDatabase) {
        this.myDatabase = myDatabase;
    }

    /**
     * Input doc will have the form:
     * {
     *     id: 1,
     *     version: 1,
     *     listOfDates: [ 1510858845481, 1510858843416, 251085884789],
     *     metadata: {
     *         accountName: customer1,
     *         eventType: biz_txn_v1,
     *         retentionPeriod: 10,
     *         pickupTimestamp: 1510858845481
     *     }
     * }
     */
    public void indexDocument(String doc) {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<Object, Object> databaseDoc = new HashMap<>();
        Map<Object, Object> metadataMap = new HashMap<>();
        databaseDoc.put("metadata", metadataMap);

        JsonNode docAsJson;
        try {
            docAsJson = objectMapper.readTree(doc);
        } catch (IOException e) {
            throw new RuntimeException();
        }

        databaseDoc.put("id", docAsJson.get("id"));

        int newVersion = docAsJson.get("version").asInt() + 1;
        databaseDoc.put("version", newVersion);

        JsonNode listOfDates = docAsJson.get("listOfDates");
        List<String> iso8601Dates = new ArrayList<>();
        int dateSize = listOfDates.size();
        for (int i = 0; i < dateSize; i++) {
            long date = listOfDates.get(i).asLong();
            // only fill in if it's before this date
            if (date < 1510777100391L) {
                ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(date), ZoneOffset.UTC);
                iso8601Dates.add(zonedDateTime.toString());
            }
        }
        databaseDoc.put("isoDates", iso8601Dates);

        List<Long> truncatedToDay = new ArrayList<>();
        for (int i = 0; i < dateSize; i++) {
            long date = listOfDates.get(i).asLong();

            ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(date), ZoneOffset.UTC);
            ZonedDateTime truncatedDateTime = zonedDateTime.truncatedTo(ChronoUnit.DAYS);
            truncatedToDay.add(truncatedDateTime.toInstant().toEpochMilli());
        }
        databaseDoc.put("truncatedDates", truncatedToDay);

        fillInAccountName(docAsJson, metadataMap);
        fillInEventType(docAsJson, metadataMap);
        fillInRetentionPeriod(docAsJson, metadataMap);
        fillInPickupTimestamp(docAsJson, metadataMap);

        myDatabase.indexDocument(databaseDoc);
    }

    private void fillInAccountName(JsonNode docAsJson, Map<Object, Object> databaseDoc) {
        String accountName = docAsJson.get("metadata").get("accountName").asText();
        String normalized = accountName.toLowerCase();
        databaseDoc.put("accountName", normalized);
    }

    private void fillInEventType(JsonNode docAsJson, Map<Object, Object> metadataDoc) {
        String eventType = docAsJson.get("metadata").get("eventType").asText();
        metadataDoc.put("eventType", eventType);
    }

    private void fillInRetentionPeriod(JsonNode docAsJson, Map<Object, Object> metadataDoc) {
        int retentionPeriod = docAsJson.get("metadata").get("retentionPeriod").asInt();
        if (retentionPeriod > 5) {
            metadataDoc.put("retentionPeriod", retentionPeriod);
        }
    }

    private void fillInPickupTimestamp(JsonNode docAsJson, Map<Object, Object> metadataDoc) {
        long retentionPeriod = docAsJson.get("metadata").get("pickupTimestamp").asLong();
        metadataDoc.put("retentionPeriod", retentionPeriod);
    }
}
