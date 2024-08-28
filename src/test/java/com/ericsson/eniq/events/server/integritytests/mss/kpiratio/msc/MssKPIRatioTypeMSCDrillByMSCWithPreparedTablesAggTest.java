/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
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
public class MssKPIRatioTypeMSCDrillByMSCWithPreparedTablesAggTest extends
        MssTestsWithTemporaryTablesBaseTestCase<MssKPIRatioTypeMSCDrillTypeMSCResult> {
    private MssKPIRatioResource mssKPIRatioResource;

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        mssKPIRatioResource = new MssKPIRatioResource();
        attachDependenciesForMSSBaseResource(mssKPIRatioResource);
    }

    private final Collection<String> getAggregationColumns() {
        final Collection<String> columnsForAggTable = new ArrayList<String>();
        columnsForAggTable.clear();
        columnsForAggTable.add(EVENT_ID);
        columnsForAggTable.add(EVNTSRC_ID);
        columnsForAggTable.add(RAT);
        columnsForAggTable.add(HIER3_ID);
        columnsForAggTable.add(NO_OF_SUCCESSES);
        columnsForAggTable.add(NO_OF_ERRORS);
        columnsForAggTable.add(NO_OF_TOTAL_ERR_SUBSCRIBERS);
        columnsForAggTable.add(DATETIME_ID);
        return columnsForAggTable;
    }

    private void createLOCAggregationTables_15MIN() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_EVENT_E_MSS_LOC_SERVICE_EVENTID_EVNTSRC_HIER3_ERR_15MIN, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_MSS_LOC_SERVICE_EVENTID_EVNTSRC_HIER3_SUC_15MIN, columnsForAggTable);
    }

    private void createLOCAggregationTables_DAY() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_EVENT_E_MSS_LOC_SERVICE_EVENTID_EVNTSRC_HIER3_ERR_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_MSS_LOC_SERVICE_EVENTID_EVNTSRC_HIER3_SUC_DAY, columnsForAggTable);
    }

    private void creatSMSAggregationTables_15MIN() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_EVENT_E_MSS_SMS_EVENTID_EVNTSRC_HIER3_ERR_15MIN, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_MSS_SMS_EVENTID_EVNTSRC_HIER3_SUC_15MIN, columnsForAggTable);
    }

    private void creatSMSAggregationTables_DAY() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_EVENT_E_MSS_SMS_EVENTID_EVNTSRC_HIER3_ERR_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_MSS_SMS_EVENTID_EVNTSRC_HIER3_SUC_DAY, columnsForAggTable);
    }

    private void creatVoiceAggregationTables_15MIN() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_EVENT_E_MSS_VOICE_EVENTID_EVNTSRC_HIER3_ERR_15MIN, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_MSS_VOICE_EVENTID_EVNTSRC_HIER3_DROP_CALL_15MIN, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_MSS_VOICE_EVENTID_EVNTSRC_HIER3_SUC_15MIN, columnsForAggTable);
    }

    private void creatVoiceAggregationTables_DAY() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_EVENT_E_MSS_VOICE_EVENTID_EVNTSRC_HIER3_ERR_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_MSS_VOICE_EVENTID_EVNTSRC_HIER3_DROP_CALL_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_MSS_VOICE_EVENTID_EVNTSRC_HIER3_SUC_DAY, columnsForAggTable);
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

    private void populateERRAggregationTablesForQuery(final String eventId, final String timestamp,
            final String tableName) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        values.put(HIER3_ID, TEST_VALUE_MSS_HIER3_ID);
        values.put(RAT, RAT_FOR_GSM);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(NO_OF_ERRORS, 1);
        values.put(NO_OF_TOTAL_ERR_SUBSCRIBERS, 1);
        values.put(DATETIME_ID, timestamp);

        insertRow(tableName, values);
    }

    private void populateSUCAggregationTablesForQuery(final String eventId, final String timestamp,
            final String tableName) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        values.put(HIER3_ID, TEST_VALUE_MSS_HIER3_ID);
        values.put(RAT, RAT_FOR_GSM);
        values.put(NO_OF_SUCCESSES, 1);
        values.put(NO_OF_ERRORS, 0);
        values.put(NO_OF_TOTAL_ERR_SUBSCRIBERS, 1);
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
            final int total, final String successRatio) {
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
    public void testTypeMSCDrilltypeMSC_LOC_Agg_15MIN() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);
        final String eventId = MSS_LOCATION_SERVICE_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_LOC;
        final int noOfErrors = 1;
        final int noOfSuccess = 1;
        final int total = 2;
        final String successRatio = "50.00";

        createLOCAggregationTables_15MIN();
        createTopologyTables();

        populateERRAggregationTablesForQuery(eventId, timestamp,
                TEMP_EVENT_E_MSS_LOC_SERVICE_EVENTID_EVNTSRC_HIER3_ERR_15MIN);
        populateSUCAggregationTablesForQuery(eventId, timestamp,
                TEMP_EVENT_E_MSS_LOC_SERVICE_EVENTID_EVNTSRC_HIER3_SUC_15MIN);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, ONE_DAY);
        final List<MssKPIRatioTypeMSCDrillTypeMSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioTypeMSCDrillTypeMSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio);
    }

    @Test
    public void testTypeMSCDrilltypeMSC_LOC_Agg_DAY() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        final String eventId = MSS_LOCATION_SERVICE_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_LOC;
        final int noOfErrors = 1;
        final int noOfSuccess = 1;
        final int total = 2;
        final String successRatio = "50.00";

        createLOCAggregationTables_DAY();
        createTopologyTables();

        populateERRAggregationTablesForQuery(eventId, timestamp,
                TEMP_EVENT_E_MSS_LOC_SERVICE_EVENTID_EVNTSRC_HIER3_ERR_DAY);
        populateSUCAggregationTablesForQuery(eventId, timestamp,
                TEMP_EVENT_E_MSS_LOC_SERVICE_EVENTID_EVNTSRC_HIER3_SUC_DAY);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, "100080");
        final List<MssKPIRatioTypeMSCDrillTypeMSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioTypeMSCDrillTypeMSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio);
    }

    @Test
    public void testTypeMSCDrilltypeMSC_SMS_msOriginating_Agg_15MIN() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);
        final String eventId = MSS_SMS_MS_ORIGINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_SMS_MS_ORIGINATING;
        final int noOfErrors = 1;
        final int noOfSuccess = 1;
        final int total = 2;
        final String successRatio = "50.00";

        creatSMSAggregationTables_15MIN();
        createTopologyTables();

        populateERRAggregationTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_SMS_EVENTID_EVNTSRC_HIER3_ERR_15MIN);
        populateSUCAggregationTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_SMS_EVENTID_EVNTSRC_HIER3_SUC_15MIN);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, ONE_DAY);
        final List<MssKPIRatioTypeMSCDrillTypeMSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioTypeMSCDrillTypeMSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio);
    }

    @Test
    public void testTypeMSCDrilltypeMSC_SMS_msOriginating_Agg_DAY() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        final String eventId = MSS_SMS_MS_ORIGINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_SMS_MS_ORIGINATING;
        final int noOfErrors = 1;
        final int noOfSuccess = 1;
        final int total = 2;
        final String successRatio = "50.00";

        creatSMSAggregationTables_DAY();
        createTopologyTables();

        populateERRAggregationTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_SMS_EVENTID_EVNTSRC_HIER3_ERR_DAY);
        populateSUCAggregationTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_SMS_EVENTID_EVNTSRC_HIER3_SUC_DAY);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, "100090");
        final List<MssKPIRatioTypeMSCDrillTypeMSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioTypeMSCDrillTypeMSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio);
    }

    @Test
    public void testTypeMSCDrilltypeMSC_SMS_msTerminating_Agg_15MIN() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);
        final String eventId = MSS_SMS_MS_TERMINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_SMS_MS_TERMINATING;
        final int noOfErrors = 1;
        final int noOfSuccess = 1;
        final int total = 2;
        final String successRatio = "50.00";

        creatSMSAggregationTables_15MIN();
        createTopologyTables();

        populateERRAggregationTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_SMS_EVENTID_EVNTSRC_HIER3_ERR_15MIN);
        populateSUCAggregationTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_SMS_EVENTID_EVNTSRC_HIER3_SUC_15MIN);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, ONE_DAY);
        final List<MssKPIRatioTypeMSCDrillTypeMSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioTypeMSCDrillTypeMSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio);
    }

    @Test
    public void testTypeMSCDrilltypeMSC_SMS_msTerminating_Agg_DAY() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        final String eventId = MSS_SMS_MS_TERMINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_SMS_MS_TERMINATING;
        final int noOfErrors = 1;
        final int noOfSuccess = 1;
        final int total = 2;
        final String successRatio = "50.00";

        creatSMSAggregationTables_DAY();
        createTopologyTables();

        populateERRAggregationTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_SMS_EVENTID_EVNTSRC_HIER3_ERR_DAY);
        populateSUCAggregationTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_SMS_EVENTID_EVNTSRC_HIER3_SUC_DAY);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, "100090");
        final List<MssKPIRatioTypeMSCDrillTypeMSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioTypeMSCDrillTypeMSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio);
    }

    @Test
    public void testTypeMSCDrilltypeMSC_VOICE_msTerminating_Agg_15MIN() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);
        final String eventId = MSS_MS_TERMINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_MS_TERMINATING;
        final int noOfErrors = 2;
        final int noOfSuccess = 1;
        final int total = 3;
        final String successRatio = "33.33";

        creatVoiceAggregationTables_15MIN();
        createTopologyTables();

        populateERRAggregationTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_EVENTID_EVNTSRC_HIER3_ERR_15MIN);
        populateERRAggregationTablesForQuery(eventId, timestamp,
                TEMP_EVENT_E_MSS_VOICE_EVENTID_EVNTSRC_HIER3_DROP_CALL_15MIN);
        populateSUCAggregationTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_EVENTID_EVNTSRC_HIER3_SUC_15MIN);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, ONE_DAY);
        final List<MssKPIRatioTypeMSCDrillTypeMSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioTypeMSCDrillTypeMSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio);
    }

    @Test
    public void testTypeMSCDrilltypeMSC_VOICE_msTerminating_Agg_DAY() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        final String eventId = MSS_MS_TERMINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_MS_TERMINATING;
        final int noOfErrors = 2;
        final int noOfSuccess = 1;
        final int total = 3;
        final String successRatio = "33.33";

        creatVoiceAggregationTables_DAY();
        createTopologyTables();

        populateERRAggregationTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_EVENTID_EVNTSRC_HIER3_ERR_DAY);
        populateERRAggregationTablesForQuery(eventId, timestamp,
                TEMP_EVENT_E_MSS_VOICE_EVENTID_EVNTSRC_HIER3_DROP_CALL_DAY);
        populateSUCAggregationTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_EVENTID_EVNTSRC_HIER3_SUC_DAY);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, "100090");
        final List<MssKPIRatioTypeMSCDrillTypeMSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioTypeMSCDrillTypeMSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio);
    }
}
