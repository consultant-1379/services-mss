package com.ericsson.eniq.events.server.integritytests.mss.kpiratio.msc;

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
import com.ericsson.eniq.events.server.resources.mss.MssKPIRatioResource;
import com.ericsson.eniq.events.server.test.queryresults.mss.MssKPIRatioTypeMSCDrillTypeMSCResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * 
 * @author echimma
 * @since 2011
 *
 */
public class MssKPIRatioTypeMSCDrillByMSCWithPreparedTablesRawTest extends
        MssTestsWithTemporaryTablesBaseTestCase<MssKPIRatioTypeMSCDrillTypeMSCResult> {
    private MssKPIRatioResource mssKPIRatioResource;

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        mssKPIRatioResource = new MssKPIRatioResource();
        attachDependenciesForMSSBaseResource(mssKPIRatioResource);
    }

    private final Collection<String> getAggregationColumns() {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.clear();
        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(EVNTSRC_ID);
        columnsForRawTable.add(HIER3_ID);
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(DATETIME_ID);
        return columnsForRawTable;
    }

    private void createLOCRawTables() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_ERR_RAW, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_SUC_RAW, columnsForAggTable);
    }

    private void creatSMSRawTables() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_EVENT_E_MSS_SMS_CDR_ERR_RAW, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_MSS_SMS_CDR_SUC_RAW, columnsForAggTable);
    }

    private void creatVoiceRawTables() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, columnsForAggTable);
    }

    private void createTopologyTables() throws Exception {
        final Collection<String> columnsForTopologyTable = new ArrayList<String>();
        columnsForTopologyTable.add(EVENT_ID);
        columnsForTopologyTable.add("EVENT_ID_DESC");
        createTemporaryTable(TEMP_DIM_E_MSS_EVENTTYPE, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(VENDOR);
        columnsForTopologyTable.add(HIERARCHY_3);
        columnsForTopologyTable.add(HIER3_ID);

        createTemporaryTable(TEMP_DIM_E_SGEH_HIER321, columnsForTopologyTable);
        createTemporaryTable(TEMP_DIM_Z_SGEH_HIER321, columnsForTopologyTable);
    }

    private void populateRawTablesForQuery(final String eventId, final String timestamp, final String tableName)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        values.put(HIER3_ID, TEST_VALUE_MSS_HIER3_ID);
        values.put(TAC, SAMPLE_TAC);
        values.put(IMSI, TEST_VALUE_IMSI);
        values.put(DATETIME_ID, timestamp);

        insertRow(tableName, values);
    }

    private void populateTopologyTablesForQuery(final String eventIdDesc, final String eventId) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put("EVENT_ID_DESC", eventIdDesc);
        values.put(EVENT_ID, eventId);
        insertRow(TEMP_DIM_E_MSS_EVENTTYPE, values);

        values.clear();
        values.put(VENDOR, GSM);
        values.put(HIERARCHY_3, SAMPLE_BSC);
        values.put(HIER3_ID, TEST_VALUE_MSS_HIER3_ID);
        insertRow(TEMP_DIM_E_SGEH_HIER321, values);
    }

    private void validateResultFromTables(final List<MssKPIRatioTypeMSCDrillTypeMSCResult> results,
            final String eventId, final String eventType, final int noOfErrors, final int noOfSuccesses,
            final int total, final String successRatio, final int impactedSubScribers) {
        assertThat(results.size(), is(1));
        final MssKPIRatioTypeMSCDrillTypeMSCResult result = results.get(0);

        assertThat(result.getEventId(), is(eventId));
        assertThat(result.getEventType(), is(eventType));
        assertThat(result.getVendor(), is(GSM));
        assertThat(result.getEvntsrcId(), is(String.valueOf(TEST_VALUE_MSS_EVNTSRC_ID)));
        assertThat(result.getHier3Id(), is(String.valueOf(TEST_VALUE_MSS_HIER3_ID)));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getNoOfErrors(), is(noOfErrors));
        assertThat(result.getNoOfSuccesses(), is(noOfSuccesses));
        assertThat(result.getTotal(), is(total));
        assertThat(result.getSuccessRatio(), is(successRatio));
        assertThat(result.getImpactedSubScribers(), is(impactedSubScribers));
    }

    private String getJsonString(final String eventID, final String timerange) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, timerange);
        map.putSingle(EVENT_ID_PARAM, eventID);
        map.putSingle(EVNTSRC_ID, String.valueOf(TEST_VALUE_MSS_EVNTSRC_ID));
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "50");

        DummyUriInfoImpl.setUriInfoMss(map, mssKPIRatioResource);

        return mssKPIRatioResource.getData();
    }

    @Test
    public void testTypeMSCDrilltypeMSC_LOC_RAW() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(15);
        final String eventId = MSS_LOCATION_SERVICE_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_LOC;
        final int noOfErrors = 1;
        final int noOfSuccess = 1;
        final int total = 2;
        final String successRatio = "50.00";
        final int impactedSubScribers = 1;

        createLOCRawTables();
        createTopologyTables();

        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_ERR_RAW);
        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_SUC_RAW);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, THIRTY_MINUTES);
        final List<MssKPIRatioTypeMSCDrillTypeMSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioTypeMSCDrillTypeMSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio,
                impactedSubScribers);
    }

    @Test
    public void testTypeMSCDrilltypeMSC_SMS_msOriginatingRAW() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(15);
        final String eventId = MSS_SMS_MS_ORIGINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_SMS_MS_ORIGINATING;
        final int noOfErrors = 1;
        final int noOfSuccess = 1;
        final int total = 2;
        final String successRatio = "50.00";
        final int impactedSubScribers = 1;

        creatSMSRawTables();
        createTopologyTables();

        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_SMS_CDR_ERR_RAW);
        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_SMS_CDR_SUC_RAW);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, THIRTY_MINUTES);
        final List<MssKPIRatioTypeMSCDrillTypeMSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioTypeMSCDrillTypeMSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio,
                impactedSubScribers);
    }

    @Test
    public void testTypeMSCDrilltypeMSC_SMS_msTerminatingRAW() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(15);
        final String eventId = MSS_SMS_MS_TERMINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_SMS_MS_TERMINATING;
        final int noOfErrors = 1;
        final int noOfSuccess = 1;
        final int total = 2;
        final String successRatio = "50.00";
        final int impactedSubScribers = 1;

        creatSMSRawTables();
        createTopologyTables();

        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_SMS_CDR_ERR_RAW);
        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_SMS_CDR_SUC_RAW);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, THIRTY_MINUTES);
        final List<MssKPIRatioTypeMSCDrillTypeMSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioTypeMSCDrillTypeMSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio,
                impactedSubScribers);
    }

    @Test
    public void testTypeMSCDrilltypeMSC_Voice_msTerminatingRAW() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(15);
        final String eventId = MSS_MS_TERMINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_MS_TERMINATING;
        final int noOfErrors = 2;
        final int noOfSuccess = 1;
        final int total = 3;
        final String successRatio = "33.33";
        final int impactedSubScribers = 1;

        creatVoiceRawTables();
        createTopologyTables();

        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW);
        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW);
        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, THIRTY_MINUTES);
        final List<MssKPIRatioTypeMSCDrillTypeMSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioTypeMSCDrillTypeMSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio,
                impactedSubScribers);
    }

    @Test
    public void testTypeMSCDrilltypeMSC_Voice_msOriginatingRAW() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(15);
        final String eventId = MSS_MS_ORIGINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_MS_ORIGINATING;
        final int noOfErrors = 2;
        final int noOfSuccess = 1;
        final int total = 3;
        final String successRatio = "33.33";
        final int impactedSubScribers = 1;

        creatVoiceRawTables();
        createTopologyTables();

        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW);
        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW);
        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, THIRTY_MINUTES);
        final List<MssKPIRatioTypeMSCDrillTypeMSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioTypeMSCDrillTypeMSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio,
                impactedSubScribers);
    }
}
