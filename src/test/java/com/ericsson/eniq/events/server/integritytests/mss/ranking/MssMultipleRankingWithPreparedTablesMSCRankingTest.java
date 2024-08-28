package com.ericsson.eniq.events.server.integritytests.mss.ranking;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.integritytests.mss.MssTestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.resources.mss.MssMultipleRankingResource;
import com.ericsson.eniq.events.server.test.queryresults.mss.MssMultipleRankingMSCResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MssMultipleRankingWithPreparedTablesMSCRankingTest extends
        MssTestsWithTemporaryTablesBaseTestCase<MssMultipleRankingMSCResult> {
    private MssMultipleRankingResource mssMultipleRankingResource;

    private static final String BLOCKED_CALLS_URI = "MSS/NETWORK/RANKING_ANALYSIS/VOICE/BLOCKED_CALLS";

    private static final String DROP_CALLS_URI = "MSS/NETWORK/RANKING_ANALYSIS/VOICE/DROPPED_CALLS";

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String MAX_ROWS_VALUE = "500";

    private static final String TZ_OFFSET_VALUE = "+0100";

    /**
     * 
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        mssMultipleRankingResource = new MssMultipleRankingResource();
        attachDependenciesForMSSBaseResource(mssMultipleRankingResource);
    }

    /**
     * 
     * @param eventID
     * @param timerange
     * @return
     * @throws Exception
     */
    private String getJsonString(final String uri, final String timerange, final String errorType) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, timerange);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_VALUE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, uri);

        return mssMultipleRankingResource.getData(VOICE, errorType);
    }

    /**
     * 
     * @return
     */
    private final Collection<String> getRawColumns() {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.clear();
        columnsForRawTable.add(EVNTSRC_ID);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(TAC);
        return columnsForRawTable;
    }

    /**
     * 
     * @return
     */
    private final Collection<String> getAggColumns() {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.clear();
        columnsForRawTable.add(EVNTSRC_ID);
        columnsForRawTable.add(NO_OF_ERRORS);
        columnsForRawTable.add(NO_OF_SUCCESSES);
        columnsForRawTable.add(DATETIME_ID);
        return columnsForRawTable;
    }

    private void createVoiceRawTables(final String sucTable, final String errTable) throws Exception {
        final Collection<String> columnsForRawTable = getRawColumns();

        createTemporaryTable(errTable, columnsForRawTable);
        createTemporaryTable(sucTable, columnsForRawTable);
    }

    private void createTopologyTables() throws Exception {
        final Collection<String> columnsForTopologyTable = new ArrayList<String>();
        columnsForTopologyTable.add(EVNTSRC_ID);
        columnsForTopologyTable.add(VENDOR);
        columnsForTopologyTable.add(EVENT_SOURCE_NAME);
        createTemporaryTableMss(TEMP_DIM_E_MSS_EVNTSRC, columnsForTopologyTable);
    }

    private void populateVoiceRawData(final String timestamp, final String sucTable, final String errTable)
            throws SQLException {
        populateVoiceRawTablesForQuery(timestamp, sucTable);
        populateVoiceRawTablesForQuery(timestamp, errTable);

        populateTopologyTablesForQuery();
    }

    /**
     * @param timestamp
     * @param sucTable
     * @param errTable
     * @throws SQLException 
     */
    private void populateVoiceAggData(final String timestamp, final String sucTable, final String errTable)
            throws SQLException {
        populateVoiceSucAggTablesForQuery(timestamp, sucTable);
        populateVoiceErrAggTablesForQuery(timestamp, errTable);

        populateTopologyTablesForQuery();
    }

    /**
     * @throws SQLException 
     * 
     */
    private void populateTopologyTablesForQuery() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        values.put(VENDOR, ERICSSON);
        values.put(EVENT_SOURCE_NAME, TEST_VALUE_MSS_EVNTSRC);
        insertRow(TEMP_DIM_E_MSS_EVNTSRC, values);
    }

    /**
     * @param timestamp
     * @param tableName
     * @throws SQLException 
     */
    private void populateVoiceRawTablesForQuery(final String timestamp, final String tableName) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        values.put(DATETIME_ID, timestamp);
        values.put(TAC, SAMPLE_TAC);
        insertRow(tableName, values);
    }

    /**
     * @param timestamp
     * @param tableName
     * @throws SQLException 
     */
    private void populateVoiceSucAggTablesForQuery(final String timestamp, final String tableName) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        values.put(NO_OF_SUCCESSES, 1);
        values.put(NO_OF_ERRORS, 0);
        values.put(DATETIME_ID, timestamp);
        insertRow(tableName, values);
    }

    /**
     * @param timestamp
     * @param tableName
     * @throws SQLException 
     */
    private void populateVoiceErrAggTablesForQuery(final String timestamp, final String tableName) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(NO_OF_ERRORS, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(tableName, values);
    }

    private void validateResultFromTables(final List<MssMultipleRankingMSCResult> results) {
        assertThat(results.size(), is(1));
        final MssMultipleRankingMSCResult result = results.get(0);
        assertThat(result.getEvntsrcId(), is(String.valueOf(TEST_VALUE_MSS_EVNTSRC_ID)));
        assertThat(result.getRank(), is(1));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getMsc(), is(TEST_VALUE_MSS_EVNTSRC));
        assertThat(result.getFailures(), is(1));
        assertThat(result.getSuccesses(), is(1));
    }

    /**
     * @param sucTable
     * @param errTable
     * @throws Exception 
     */
    private void createVoiceAggTables(final String sucTable, final String errTable) throws Exception {
        final Collection<String> columnsForRawTable = getAggColumns();

        createTemporaryTable(errTable, columnsForRawTable);
        createTemporaryTable(sucTable, columnsForRawTable);
    }

    @Test
    public void testMSCBlockRankingRaw() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(4);
        final String errTable = TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW;
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW;

        createVoiceRawTables(sucTable, errTable);

        createTopologyTables();

        populateVoiceRawData(timestamp, sucTable, errTable);

        final String json = getJsonString(BLOCKED_CALLS_URI, FIVE_MINUTES, BLOCKED_CALLS);

        final List<MssMultipleRankingMSCResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingMSCResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testMSCDropRankingRaw() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(4);

        final String errTable = TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW;
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW;

        createVoiceRawTables(sucTable, errTable);

        createTopologyTables();

        populateVoiceRawData(timestamp, sucTable, errTable);

        final String json = getJsonString(DROP_CALLS_URI, FIVE_MINUTES, DROPPED_CALLS);

        final List<MssMultipleRankingMSCResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingMSCResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testMSCBlockRanking15Min() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);

        final String errTable = TEMP_EVENT_E_MSS_VOICE_EVNTSRC_ERR_15MIN;
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_EVNTSRC_SUC_15MIN;

        createVoiceAggTables(sucTable, errTable);

        createTopologyTables();

        populateVoiceAggData(timestamp, sucTable, errTable);

        final String json = getJsonString(BLOCKED_CALLS_URI, ONE_DAY, BLOCKED_CALLS);

        final List<MssMultipleRankingMSCResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingMSCResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testMSCDropRanking15Min() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);

        final String errTable = TEMP_EVENT_E_MSS_VOICE_EVNTSRC_DROP_CALL_15MIN;
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_EVNTSRC_SUC_15MIN;

        createVoiceAggTables(sucTable, errTable);

        createTopologyTables();

        populateVoiceAggData(timestamp, sucTable, errTable);

        final String json = getJsonString(DROP_CALLS_URI, ONE_DAY, DROPPED_CALLS);

        final List<MssMultipleRankingMSCResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingMSCResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testMSCBlockRankingDAY() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(48);

        final String errTable = TEMP_EVENT_E_MSS_VOICE_EVNTSRC_ERR_DAY;
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_EVNTSRC_SUC_DAY;

        createVoiceAggTables(sucTable, errTable);

        createTopologyTables();

        populateVoiceAggData(timestamp, sucTable, errTable);

        final String json = getJsonString(BLOCKED_CALLS_URI, "100090", BLOCKED_CALLS);

        final List<MssMultipleRankingMSCResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingMSCResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testMSCDropRankingDAY() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(48);

        final String errTable = TEMP_EVENT_E_MSS_VOICE_EVNTSRC_DROP_CALL_DAY;
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_EVNTSRC_SUC_DAY;

        createVoiceAggTables(sucTable, errTable);

        createTopologyTables();

        populateVoiceAggData(timestamp, sucTable, errTable);

        final String json = getJsonString(DROP_CALLS_URI, "100090", DROPPED_CALLS);

        final List<MssMultipleRankingMSCResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingMSCResult.class);

        validateResultFromTables(summaryResult);
    }
}
