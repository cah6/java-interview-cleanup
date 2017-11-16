import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class IndexerServiceTest {

    @Mock
    private MyDatabase myDatabase;

    private IndexerService target;

    private String exampleDoc = "{\n" +
            "   \"id\":1,\n" +
            "   \"version\":1,\n" +
            "   \"listOfDates\":[\n" +
            "      1510858845481,\n" +
            "      1510858843416,\n" +
            "      251085884789\n" +
            "   ],\n" +
            "   \"metadata\":{\n" +
            "      \"accountName\":\"customer1\",\n" +
            "      \"eventType\":\"biz_txn_v1\",\n" +
            "      \"retentionPeriod\":10,\n" +
            "      \"pickupTimestamp\":1510858845481\n" +
            "   }\n" +
            "}";

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);

        target = new IndexerService(myDatabase);
    }

    @Test
    public void testIndexDocument() {
        target.indexDocument(exampleDoc);
    }

}