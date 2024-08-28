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
import com.ericsson.eniq.events.server.test.queryresults.mss.MssMultipleRankingICCResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MssMultipleRankingWithPreparedTablesCCVoiceRankingTest extends
        MssTestsWithTemporaryTablesBaseTestCase<MssMultipleRankingICCResult> {
    private MssMultipleRankingResource mssMultipleRankingResource;

    public static final String TOTAL_CALLS = "TOTAL_CALLS";

    private static final String TOTAL_CALLS_URI = "MSS/NETWORK/RANKING_ANALYSIS/VOICE/TOTAL_CALLS";

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
        map.putSingle(TYPE_PARAM, TYPE_INTERNAL_CAUSE_CODE);
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
        columnsForRawTable.add(INTERNAL_CAUSE_CODE);
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
        columnsForRawTable.add(INTERNAL_CAUSE_CODE);
        columnsForRawTable.add(NO_OF_ERRORS);
        columnsForRawTable.add(NO_OF_SUCCESSES);
        columnsForRawTable.add(DATETIME_ID);
        return columnsForRawTable;
    }

    private void createVoiceRawTables(final String sucTable, final String errTable, final String dropTable)
            throws Exception {
        final Collection<String> columnsForRawTable = getRawColumns();

        createTemporaryTableMss(errTable, columnsForRawTable);
        createTemporaryTableMss(sucTable, columnsForRawTable);
        createTemporaryTableMss(dropTable, columnsForRawTable);
    }

    private void createTopologyTables() throws Exception {
        final Collection<String> columnsForTopologyTable = new ArrayList<String>();
        columnsForTopologyTable.add(INTERNAL_CAUSE_CODE);
        columnsForTopologyTable.add(INTERNAL_CAUSE_CODE_DESC);

        createTemporaryTableMss(TEMP_DIM_E_MSS_INTERNAL_CAUSE_CODE, columnsForTopologyTable);
    }

    private void populateVoiceRawData(final String timestamp, final String sucTable, final String errTable,
            final String dropTable) throws SQLException {
        populateVoiceRawTablesForQuery(timestamp, sucTable);
        populateVoiceRawTablesForQuery(timestamp, errTable);
        populateVoiceRawTablesForQuery(timestamp, dropTable);

        populateTopologyTablesForQuery();
    }

    /**
     * @param timestamp
     * @param sucTable
     * @param errTable
     * @throws SQLException 
     */
    private void populateVoiceAggData(final String timestamp, final String sucTable, final String errTable,
            final String dropTable) throws SQLException {
        populateVoiceSucAggTablesForQuery(timestamp, sucTable);
        populateVoiceErrAggTablesForQuery(timestamp, errTable);
        populateVoiceErrAggTablesForQuery(timestamp, dropTable);

        populateTopologyTablesForQuery();
    }

    /**
     * @throws SQLException 
     * 
     */
    private void populateTopologyTablesForQuery() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(INTERNAL_CAUSE_CODE_DESC, TEST_VALUE_INTERNAL_CAUSE_CODE_DESC);
        values.put(INTERNAL_CAUSE_CODE, TEST_VALUE_INTERNAL_CAUSE_CODE);
        insertRow(TEMP_DIM_E_MSS_INTERNAL_CAUSE_CODE, values);
    }

    /**
     * @param timestamp
     * @param tableName
     * @throws SQLException 
     */
    private void populateVoiceRawTablesForQuery(final String timestamp, final String tableName) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(INTERNAL_CAUSE_CODE, TEST_VALUE_INTERNAL_CAUSE_CODE);
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
        values.put(INTERNAL_CAUSE_CODE, TEST_VALUE_INTERNAL_CAUSE_CODE);
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
        values.put(INTERNAL_CAUSE_CODE, TEST_VALUE_INTERNAL_CAUSE_CODE);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(NO_OF_ERRORS, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(tableName, values);
    }

    private void validateResultFromTables(final List<MssMultipleRankingICCResult> results) {
        assertThat(results.size(), is(1));
        final MssMultipleRankingICCResult result = results.get(0);
        assertThat(result.getRank(), is(1));
        assertThat(result.getICCDesc(), is(TEST_VALUE_INTERNAL_CAUSE_CODE_DESC));
        assertThat(result.getICC(), is(String.valueOf(TEST_VALUE_INTERNAL_CAUSE_CODE)));
        assertThat(result.getFailures(), is(2));
        assertThat(result.getSuccesses(), is(1));
    }

    /**
     * @param sucTable
     * @param errTable
     * @throws Exception 
     */
    private void createVoiceAggTables(final String sucTable, final String errTable, final String dropTable)
            throws Exception {
        final Collection<String> columnsForRawTable = getAggColumns();

        createTemporaryTable(errTable, columnsForRawTable);
        createTemporaryTable(sucTable, columnsForRawTable);
        createTemporaryTable(dropTable, columnsForRawTable);
    }

    @Test
    public void testICCRankingRaw() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(4);
        final String errTable = TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW;
        final String dropTable = TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW;
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW;

        createVoiceRawTables(sucTable, errTable, dropTable);

        createTopologyTables();

        populateVoiceRawData(timestamp, sucTable, errTable, dropTable);

        final String json = getJsonString(TOTAL_CALLS_URI, FIVE_MINUTES, TOTAL_CALLS);

        final List<MssMultipleRankingICCResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingICCResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testICCRanking15Min() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);

        final String errTable = TEMP_EVENT_E_MSS_VOICE_EVNTSRC_CC_ERR_15MIN;
        final String dropTable = TEMP_EVENT_E_MSS_VOICE_EVNTSRC_CC_DROP_CALL_15MIN;
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_EVNTSRC_CC_SUC_15MIN;

        createVoiceAggTables(sucTable, errTable, dropTable);

        createTopologyTables();

        populateVoiceAggData(timestamp, sucTable, errTable, dropTable);

        final String json = getJsonString(TOTAL_CALLS_URI, ONE_DAY, TOTAL_CALLS);

        final List<MssMultipleRankingICCResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingICCResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testICCRankingDAY() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(48);

        final String errTable = TEMP_EVENT_E_MSS_VOICE_EVNTSRC_CC_ERR_DAY;
        final String dropTable = TEMP_EVENT_E_MSS_VOICE_EVNTSRC_CC_DROP_CALL_DAY;
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_EVNTSRC_CC_SUC_DAY;

        createVoiceAggTables(sucTable, errTable, dropTable);

        createTopologyTables();

        populateVoiceAggData(timestamp, sucTable, errTable, dropTable);

        final String json = getJsonString(TOTAL_CALLS_URI, "100090", TOTAL_CALLS);

        final List<MssMultipleRankingICCResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingICCResult.class);

        validateResultFromTables(summaryResult);
    }
}
