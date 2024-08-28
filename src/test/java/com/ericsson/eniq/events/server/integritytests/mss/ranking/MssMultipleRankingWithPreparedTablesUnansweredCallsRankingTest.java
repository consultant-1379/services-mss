package com.ericsson.eniq.events.server.integritytests.mss.ranking;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
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
import com.ericsson.eniq.events.server.test.queryresults.mss.MssMultipleRankingUnansweredCallsResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MssMultipleRankingWithPreparedTablesUnansweredCallsRankingTest extends
        MssTestsWithTemporaryTablesBaseTestCase<MssMultipleRankingUnansweredCallsResult> {
    private MssMultipleRankingResource mssMultipleRankingResource;

    private static final String MS_ORIGINATING_UNANSWERED_URI = "MSS/NETWORK/RANKING_ANALYSIS/VOICE/MS_ORIGINATING_UNANSWERED";

    private static final String MS_TERMINATING_UNANSWERED_URI = "MSS/NETWORK/RANKING_ANALYSIS/VOICE/MS_TERMINATING_UNANSWERED";

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
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(CALLING_PARTY_NUM);
        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(CALL_POSITION);
        columnsForRawTable.add(DISCONNECT_PARTY);
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
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(CALLING_PARTY_NUM);
        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(NO_OF_UNANSWERED_CALLS);
        columnsForRawTable.add(DATETIME_ID);
        return columnsForRawTable;
    }

    private void createVoiceRawTables(final String sucTable) throws Exception {
        final Collection<String> columnsForRawTable = getRawColumns();

        createTemporaryTableMss(sucTable, columnsForRawTable);
    }

    private void populateVoiceRawData(final String timestamp, final String sucTable, final String eventId,
            final int callPosition, final int disconnectParty, final int icc) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI, TEST_VALUE_IMSI);
        values.put(CALLING_PARTY_NUM, TEST_VALUE_CALLING_PARTY_NUM);
        values.put(EVENT_ID, eventId);
        values.put(CALL_POSITION, callPosition);
        values.put(DISCONNECT_PARTY, disconnectParty);
        values.put(INTERNAL_CAUSE_CODE, icc);
        values.put(DATETIME_ID, timestamp);
        values.put(TAC, SAMPLE_TAC);
        insertRow(sucTable, values);
    }

    /**
     * @param timestamp
     * @param tableName
     * @throws SQLException 
     */
    private void populateVoiceAggTablesForQuery(final String timestamp, final String tableName, final String eventId)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI, TEST_VALUE_IMSI);
        values.put(CALLING_PARTY_NUM, TEST_VALUE_CALLING_PARTY_NUM);
        values.put(EVENT_ID, eventId);
        values.put(NO_OF_UNANSWERED_CALLS, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(tableName, values);
    }

    private void validateResultFromTables(final List<MssMultipleRankingUnansweredCallsResult> results) {
        assertThat(results.size(), is(1));
        final MssMultipleRankingUnansweredCallsResult result = results.get(0);
        assertThat(result.getRank(), is(1));
        assertThat(result.getIMSI(), is(String.valueOf(TEST_VALUE_IMSI)));
        assertThat(result.getCallingPartyNum(), is(String.valueOf(TEST_VALUE_CALLING_PARTY_NUM)));
        assertThat(result.getNoOfUnansweredCalls(), is(1));
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
    public void testMsOriginatingUnansweredCallsRankingRaw() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(4);
        final String sucTable = TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW;
        final String eventId = MSS_MS_ORIGINATING_EVENT_ID;
        final int callPosition = 2;
        final int disconnectParty = 1;
        final int icc = 3;

        createVoiceRawTables(sucTable);

        populateVoiceRawData(timestamp, sucTable, eventId, callPosition, disconnectParty, icc);

        final String json = getJsonString(MS_ORIGINATING_UNANSWERED_URI, FIVE_MINUTES, MS_ORIGINATING_UNANSWERED);

        final List<MssMultipleRankingUnansweredCallsResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingUnansweredCallsResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testMsTerminatingUnansweredCallsRankingRaw() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(4);

        final String sucTable = TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW;
        final String eventId = MSS_MS_TERMINATING_EVENT_ID;
        final int callPosition = 2;
        final int disconnectParty = 1;
        final int icc = 3;

        createVoiceRawTables(sucTable);

        populateVoiceRawData(timestamp, sucTable, eventId, callPosition, disconnectParty, icc);

        final String json = getJsonString(MS_TERMINATING_UNANSWERED_URI, FIVE_MINUTES, MS_TERMINATING_UNANSWERED);

        final List<MssMultipleRankingUnansweredCallsResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingUnansweredCallsResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testMsOriginatingUnansweredCallsRanking15Min() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);

        final String sucTable = TEMP_EVENT_E_MSS_VOICE_IMSI_UNANSWERED_CALL_RANK_15MIN;
        final String eventId = MSS_MS_ORIGINATING_EVENT_ID;

        createVoiceAggTables(sucTable);
        //Use error agg tables for IMSI ranking
        populateVoiceAggTablesForQuery(timestamp, sucTable, eventId);

        final String json = getJsonString(MS_ORIGINATING_UNANSWERED_URI, ONE_DAY, MS_ORIGINATING_UNANSWERED);

        final List<MssMultipleRankingUnansweredCallsResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingUnansweredCallsResult.class);

        validateResultFromTables(summaryResult);
    }

    @Test
    public void testMsTerminatingUnansweredCallsRanking15Min() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);

        final String sucTable = TEMP_EVENT_E_MSS_VOICE_IMSI_UNANSWERED_CALL_RANK_15MIN;
        final String eventId = MSS_MS_TERMINATING_EVENT_ID;

        createVoiceAggTables(sucTable);
        //Use error agg tables for IMSI ranking
        populateVoiceAggTablesForQuery(timestamp, sucTable, eventId);

        final String json = getJsonString(MS_TERMINATING_UNANSWERED_URI, ONE_DAY, MS_TERMINATING_UNANSWERED);

        final List<MssMultipleRankingUnansweredCallsResult> summaryResult = getTranslator().translateResult(json,
                MssMultipleRankingUnansweredCallsResult.class);

        validateResultFromTables(summaryResult);
    }
}
