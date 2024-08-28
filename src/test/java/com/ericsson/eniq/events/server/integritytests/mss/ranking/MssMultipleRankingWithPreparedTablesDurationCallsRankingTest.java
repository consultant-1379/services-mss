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
import com.ericsson.eniq.events.server.test.queryresults.mss.MssMultipleRankingDurationCallsResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MssMultipleRankingWithPreparedTablesDurationCallsRankingTest extends
        MssTestsWithTemporaryTablesBaseTestCase<MssMultipleRankingDurationCallsResult> {
    private MssMultipleRankingResource mssMultipleRankingResource;

    private static final String LONG_DURATION_CALLS_URI = "MSS/NETWORK/RANKING_ANALYSIS/VOICE/LONG_DURATION_CALLS";

    private static final String SHORT_DURATION_CALLS_URI = "MSS/NETWORK/RANKING_ANALYSIS/VOICE/SHORT_DURATION_CALLS";

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
        columnsForRawTable.add(CALL_DURATION);
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(MSISDN);
        columnsForRawTable.add(CALLING_PARTY_NUM);
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
        columnsForRawTable.add(MSISDN);
        columnsForRawTable.add(CALLING_PARTY_NUM);
        columnsForRawTable.add(CALL_DURATION);
        columnsForRawTable.add(DATETIME_ID);
        return columnsForRawTable;
    }

    private void createVoiceRawTables(final String sucTable) throws Exception {
        final Collection<String> columnsForRawTable = getRawColumns();

        createTemporaryTableMss(sucTable, columnsForRawTable);
    }

    private void populateVoiceRawData(final String timestamp, final String sucTable) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI, TEST_VALUE_IMSI);
        values.put(MSISDN, TEST_VALUE_MSISDN);
        values.put(CALLING_PARTY_NUM, TEST_VALUE_CALLING_PARTY_NUM);
        values.put(CALL_DURATION, TEST_VALUE_CALL_DURATION);
        values.put(DATETIME_ID, timestamp);
        values.put(TAC, SAMPLE_TAC);
        insertRow(sucTable, values);
    }

    /**
     * @param timestamp
     * @param tableName
     * @throws SQLException 
     */
    private void populateVoiceAggTablesForQuery(final String timestamp, final String tableName) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI, TEST_VALUE_IMSI);
        values.put(MSISDN, TEST_VALUE_MSISDN);
        values.put(CALLING_PARTY_NUM, TEST_VALUE_CALLING_PARTY_NUM);
        values.put(CALL_DURATION, TEST_VALUE_CALL_DURATION);
        values.put(DATETIME_ID, timestamp);
        insertRow(tableName, values);
    }

    private void validateResultFromTables(final List<MssMultipleRankingDurationCallsResult> results) {
        assertThat(results.size(), is(1));
        final MssMultipleRankingDurationCallsResult result = results.get(0);
        assertThat(result.getRank(), is(1));
        assertThat(result.getIMSI(), is(String.valueOf(TEST_VALUE_IMSI)));
        assertThat(result.getMSISDN(), is(String.valueOf(TEST_VALUE_MSISDN)));
        assertThat(result.getCallingPartyNum(), is(String.valueOf(TEST_VALUE_CALLING_PARTY_NUM)));
        assertThat(result.getCallDuration(), is(String.valueOf(TEST_VALUE_CALL_DURATION)));
    }

    /**
     * @param sucTable
     * @param errTable
     * @throws Exception 
     */
    private void createVoiceAggTables(final String errTable) throws Exception {
        final Collection<String> columnsForRawTable = getAggColumns();

        createTemporaryTableMss(errTable, columnsForRawTable);
    }

    @Test
    public void testIMSILongDurationRankingRaw() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(4);
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW;

        createVoiceRawTables(sucTable);

        populateVoiceRawData(timestamp, sucTable);

        final String json = getJsonString(LONG_DURATION_CALLS_URI, FIVE_MINUTES, LONG_DURATION_CALLS);

        final List<MssMultipleRankingDurationCallsResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingDurationCallsResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testIMSIShortDurationRankingRaw() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(4);

        final String sucTable = TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW;

        createVoiceRawTables(sucTable);

        populateVoiceRawData(timestamp, sucTable);

        final String json = getJsonString(SHORT_DURATION_CALLS_URI, FIVE_MINUTES, SHORT_DURATION_CALLS);

        final List<MssMultipleRankingDurationCallsResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingDurationCallsResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testIMSILongDurationRanking15Min() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);

        final String sucTable = TEMP_EVENT_E_MSS_VOICE_IMSI_LONG_DUR_RANK_15MIN;

        createVoiceAggTables(sucTable);
        //Use error agg tables for IMSI ranking
        populateVoiceAggTablesForQuery(timestamp, sucTable);

        final String json = getJsonString(LONG_DURATION_CALLS_URI, ONE_DAY, LONG_DURATION_CALLS);

        final List<MssMultipleRankingDurationCallsResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingDurationCallsResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testIMSIShortDurationRanking15Min() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);

        final String sucTable = TEMP_EVENT_E_MSS_VOICE_IMSI_SHORT_DUR_RANK_15MIN;

        createVoiceAggTables(sucTable);
        //Use error agg tables for IMSI ranking
        populateVoiceAggTablesForQuery(timestamp, sucTable);

        final String json = getJsonString(SHORT_DURATION_CALLS_URI, ONE_DAY, SHORT_DURATION_CALLS);

        final List<MssMultipleRankingDurationCallsResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingDurationCallsResult.class);

        validateResultFromTables(summaryResult);
    }
}