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
import com.ericsson.eniq.events.server.test.queryresults.mss.MssMultipleRankingSubscriberResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MssMultipleRankingWithPreparedTablesSubscriberVoiceRankingTest extends
        MssTestsWithTemporaryTablesBaseTestCase<MssMultipleRankingSubscriberResult> {
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
     * @param timerange
     * @return
     * @throws Exception
     */
    private String getJsonString(final String uri, final String timerange, final String errorType) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, IMSI_PARAM_UPPER_CASE);
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
        columnsForRawTable.add(IMSI);
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
        columnsForRawTable.add(IMSI);
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

    private void createVoiceRawTables(final String sucTable) throws Exception {
        final Collection<String> columnsForRawTable = getRawColumns();

        createTemporaryTable(sucTable, columnsForRawTable);
    }

    private void populateVoiceRawData(final String timestamp, final String sucTable, final String errTable)
            throws SQLException {
        populateVoiceRawTablesForQuery(timestamp, sucTable, TEST_VALUE_IMSI);
        populateVoiceRawTablesForQuery(timestamp, errTable, TEST_VALUE_IMSI);
        populateVoiceRawTablesForQuery(timestamp, sucTable, TEST_VALUE_IMSI_ZERO);
    }

    /**
     * @param timestamp
     * @param tableName
     * @throws SQLException 
     */
    private void populateVoiceRawTablesForQuery(final String timestamp, final String tableName, final long imsi)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI, imsi);
        values.put(DATETIME_ID, timestamp);
        values.put(TAC, SAMPLE_TAC);
        insertRow(tableName, values);
    }

    /**
     * @param timestamp
     * @param tableName
     * @throws SQLException 
     */
    private void populateVoiceErrAggTablesForQuery(final String timestamp, final String tableName, final long imsi)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI, imsi);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(NO_OF_ERRORS, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(tableName, values);
    }

    private void validateResultFromTables(final List<MssMultipleRankingSubscriberResult> results) {
        assertThat(results.size(), is(1));
        final MssMultipleRankingSubscriberResult result = results.get(0);
        assertThat(result.getRank(), is(1));
        assertThat(result.getIMSI(), is(String.valueOf(TEST_VALUE_IMSI)));
        assertThat(result.getFailures(), is(1));
        assertThat(result.getSuccesses(), is(1));
    }

    /**
     * @param errTable
     * @throws Exception 
     */
    private void createVoiceAggTables(final String errTable) throws Exception {
        final Collection<String> columnsForRawTable = getAggColumns();

        createTemporaryTable(errTable, columnsForRawTable);
    }

    @Test
    public void testIMSIBlockRankingRaw() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(4);
        final String errTable = TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW;
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW;

        createVoiceRawTables(sucTable, errTable);

        populateVoiceRawData(timestamp, sucTable, errTable);

        final String json = getJsonString(BLOCKED_CALLS_URI, FIVE_MINUTES, BLOCKED_CALLS);

        final List<MssMultipleRankingSubscriberResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingSubscriberResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testIMSIDropRankingRaw() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(4);

        final String errTable = TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW;
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW;

        createVoiceRawTables(sucTable, errTable);

        populateVoiceRawData(timestamp, sucTable, errTable);

        final String json = getJsonString(DROP_CALLS_URI, FIVE_MINUTES, DROPPED_CALLS);

        final List<MssMultipleRankingSubscriberResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingSubscriberResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testIMSIBlockRanking15Min() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);

        final String errTable = TEMP_EVENT_E_MSS_VOICE_IMSI_RANK_15MIN;
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW;

        createVoiceAggTables(errTable);
        createVoiceRawTables(sucTable);

        //Use Success raw tables for IMSI ranking all the time
        populateVoiceRawTablesForQuery(timestamp, sucTable, TEST_VALUE_IMSI);
        populateVoiceRawTablesForQuery(timestamp, sucTable, TEST_VALUE_IMSI_ZERO);
        //Use error agg tables for IMSI ranking
        populateVoiceErrAggTablesForQuery(timestamp, errTable, TEST_VALUE_IMSI);
        populateVoiceErrAggTablesForQuery(timestamp, errTable, TEST_VALUE_IMSI_ZERO);

        final String json = getJsonString(BLOCKED_CALLS_URI, ONE_DAY, BLOCKED_CALLS);

        final List<MssMultipleRankingSubscriberResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingSubscriberResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testIMSIDropRanking15Min() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);

        final String errTable = TEMP_EVENT_E_MSS_VOICE_IMSI_RANK_DROP_CALL_15MIN;
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW;

        createVoiceAggTables(errTable);
        createVoiceRawTables(sucTable);

        //Use Success raw tables for IMSI ranking all the time
        populateVoiceRawTablesForQuery(timestamp, sucTable, TEST_VALUE_IMSI);
        populateVoiceRawTablesForQuery(timestamp, sucTable, TEST_VALUE_IMSI_ZERO);
        //Use error agg tables for IMSI ranking
        populateVoiceErrAggTablesForQuery(timestamp, errTable, TEST_VALUE_IMSI);
        populateVoiceErrAggTablesForQuery(timestamp, errTable, TEST_VALUE_IMSI_ZERO);

        final String json = getJsonString(DROP_CALLS_URI, ONE_DAY, DROPPED_CALLS);

        final List<MssMultipleRankingSubscriberResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingSubscriberResult.class);

        validateResultFromTables(summaryResult);
    }
}
