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
import com.ericsson.eniq.events.server.test.queryresults.mss.MssKPIRatioDrillTypeBSCResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * 
 * @author echimma
 * @since 2011
 *
 */
public class MssKPIRatioTypeMSCDrillByBSCWithPreparedTablesAggTest extends
        MssTestsWithTemporaryTablesBaseTestCase<MssKPIRatioDrillTypeBSCResult> {
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
        columnsForAggTable.add(HIER3_ID);
        columnsForAggTable.add(HIER321_ID);
        columnsForAggTable.add(RAT);
        columnsForAggTable.add(NO_OF_SUCCESSES);
        columnsForAggTable.add(NO_OF_ERRORS);
        columnsForAggTable.add(NO_OF_TOTAL_ERR_SUBSCRIBERS);
        columnsForAggTable.add(DATETIME_ID);
        return columnsForAggTable;
    }

    private void createTopologyTables() throws Exception {
        final Collection<String> columnsForTopologyTable = new ArrayList<String>();
        columnsForTopologyTable.add(EVENT_ID);
        columnsForTopologyTable.add("EVENT_ID_DESC");
        createTemporaryTable(TEMP_DIM_E_MSS_EVENTTYPE, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(VENDOR);
        columnsForTopologyTable.add(HIERARCHY_1);
        columnsForTopologyTable.add(HIER3_ID);
        columnsForTopologyTable.add(HIER321_ID);

        createTemporaryTable(TEMP_DIM_E_SGEH_HIER321, columnsForTopologyTable);
        createTemporaryTable(TEMP_DIM_Z_SGEH_HIER321, columnsForTopologyTable);
    }

    private void populateERRAggregationTablesForQuery(final String eventId, final String timestamp,
            final String tableName) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        values.put(HIER3_ID, TEST_VALUE_MSS_HIER3_ID);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
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
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        values.put(RAT, RAT_FOR_GSM);
        values.put(NO_OF_SUCCESSES, 1);
        values.put(NO_OF_ERRORS, 0);
        values.put(NO_OF_TOTAL_ERR_SUBSCRIBERS, 0);
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
        values.put(HIERARCHY_1, TEST_VALUE_MSS_CELL_NAME);
        values.put(HIER3_ID, TEST_VALUE_MSS_HIER3_ID);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        insertRow(TEMP_DIM_E_SGEH_HIER321, values);
    }

    private void createLOCAggregationTables_15MIN() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_LOC_CONTROLLER_CELL_SUM_SUC_15MIN, columnsForAggTable);
        createTemporaryTable(TEMP_LOC_CONTROLLER_CELL_SUM_ERR_15MIN, columnsForAggTable);
    }

    private void createLOCAggregationTables_DAY() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_LOC_CONTROLLER_CELL_SUM_SUC_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_LOC_CONTROLLER_CELL_SUM_ERR_DAY, columnsForAggTable);
    }

    private void createSMSAggregationTables_15MIN() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_SMS_CONTROLLER_CELL_SUM_ERR_15MIN, columnsForAggTable);
        createTemporaryTable(TEMP_SMS_CONTROLLER_CELL_SUM_SUC_15MIN, columnsForAggTable);
    }

    private void createSMSAggregationTables_DAY() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_SMS_CONTROLLER_CELL_SUM_ERR_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_SMS_CONTROLLER_CELL_SUM_SUC_DAY, columnsForAggTable);
    }

    private void creatVoiceAggregationTables_15MIN() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_VOICE_CONTROLLER_CELL_SUM_DROP_15MIN, columnsForAggTable);
        createTemporaryTable(TEMP_VOICE_CONTROLLER_CELL_SUM_ERR_15MIN, columnsForAggTable);
        createTemporaryTable(TEMP_VOICE_CONTROLLER_CELL_SUM_SUC_15MIN, columnsForAggTable);
    }

    private void createVoiceAggregationTables_DAY() throws Exception {
        final Collection<String> columnsForAggTable = getAggregationColumns();

        createTemporaryTable(TEMP_VOICE_CONTROLLER_CELL_SUM_DROP_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_VOICE_CONTROLLER_CELL_SUM_ERR_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_VOICE_CONTROLLER_CELL_SUM_SUC_DAY, columnsForAggTable);
    }

    private String getJsonString(final String eventID, final String timerange) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, timerange);
        map.putSingle(EVENT_ID_PARAM, eventID);
        map.putSingle(EVNTSRC_ID, String.valueOf(TEST_VALUE_MSS_EVNTSRC_ID));
        map.putSingle(HIER3_ID, String.valueOf(TEST_VALUE_MSS_HIER3_ID));
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "50");

        DummyUriInfoImpl.setUriInfoMss(map, mssKPIRatioResource);

        return mssKPIRatioResource.getData();
    }

    private void validateResultFromTables(final List<MssKPIRatioDrillTypeBSCResult> results, final String eventId,
            final String eventType, final int noOfErrors, final int noOfSuccesses, final int total,
            final String successRatio, final int impactedSubScribers) {
        assertThat(results.size(), is(1));
        final MssKPIRatioDrillTypeBSCResult result = results.get(0);

        assertThat(result.getEventId(), is(eventId));
        assertThat(result.getEventType(), is(eventType));
        assertThat(result.getVendor(), is(GSM));
        assertThat(result.getEvntsrcId(), is(String.valueOf(TEST_VALUE_MSS_EVNTSRC_ID)));
        assertThat(result.getHier3Id(), is(String.valueOf(TEST_VALUE_MSS_HIER3_ID)));
        assertThat(result.getHier321Id(), is(String.valueOf(TEST_VALUE_MSS_HIER321_ID)));
        assertThat(result.getAccessArea(), is(TEST_VALUE_MSS_CELL_NAME));
        assertThat(result.getNoOfErrors(), is(noOfErrors));
        assertThat(result.getNoOfSuccesses(), is(noOfSuccesses));
        assertThat(result.getTotal(), is(total));
        assertThat(result.getSuccessRatio(), is(successRatio));
        assertThat(result.getImpactedSubScribers(), is(impactedSubScribers));
    }

    @Test
    public void testTypeMSCDrilltypeBSC_LOC_Agg_15MIN() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);
        final String eventId = MSS_LOCATION_SERVICE_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_LOC;
        final int noOfErrors = 1;
        final int noOfSuccess = 1;
        final int total = 2;
        final String successRatio = "50.00";
        final int impactedSubScribers = 1;

        createLOCAggregationTables_15MIN();
        createTopologyTables();

        populateERRAggregationTablesForQuery(eventId, timestamp, TEMP_LOC_CONTROLLER_CELL_SUM_ERR_15MIN);
        populateSUCAggregationTablesForQuery(eventId, timestamp, TEMP_LOC_CONTROLLER_CELL_SUM_SUC_15MIN);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, ONE_DAY);
        final List<MssKPIRatioDrillTypeBSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioDrillTypeBSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio,
                impactedSubScribers);
    }

    @Test
    public void testTypeMSCDrilltypeBSC_LOC_Agg_DAY() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        final String eventId = MSS_LOCATION_SERVICE_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_LOC;
        final int noOfErrors = 1;
        final int noOfSuccess = 1;
        final int total = 2;
        final String successRatio = "50.00";
        final int impactedSubScribers = 1;

        createLOCAggregationTables_DAY();
        createTopologyTables();

        populateERRAggregationTablesForQuery(eventId, timestamp, TEMP_LOC_CONTROLLER_CELL_SUM_ERR_DAY);
        populateSUCAggregationTablesForQuery(eventId, timestamp, TEMP_LOC_CONTROLLER_CELL_SUM_SUC_DAY);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, "100090");
        final List<MssKPIRatioDrillTypeBSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioDrillTypeBSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio,
                impactedSubScribers);
    }

    @Test
    public void testTypeMSCDrilltypeBSC_SMS_msOriginating_Agg_15MIN() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);
        final String eventId = MSS_SMS_MS_ORIGINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_SMS_MS_ORIGINATING;
        final int noOfErrors = 1;
        final int noOfSuccess = 1;
        final int total = 2;
        final String successRatio = "50.00";
        final int impactedSubScribers = 1;

        createSMSAggregationTables_15MIN();
        createTopologyTables();

        populateERRAggregationTablesForQuery(eventId, timestamp, TEMP_SMS_CONTROLLER_CELL_SUM_ERR_15MIN);
        populateSUCAggregationTablesForQuery(eventId, timestamp, TEMP_SMS_CONTROLLER_CELL_SUM_SUC_15MIN);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, ONE_DAY);
        final List<MssKPIRatioDrillTypeBSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioDrillTypeBSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio,
                impactedSubScribers);
    }

    @Test
    public void testTypeMSCDrilltypeBSC_SMS_msOriginating_Agg_DAY() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        final String eventId = MSS_SMS_MS_ORIGINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_SMS_MS_ORIGINATING;
        final int noOfErrors = 1;
        final int noOfSuccess = 1;
        final int total = 2;
        final String successRatio = "50.00";
        final int impactedSubScribers = 1;

        createSMSAggregationTables_DAY();
        createTopologyTables();

        populateERRAggregationTablesForQuery(eventId, timestamp, TEMP_SMS_CONTROLLER_CELL_SUM_ERR_DAY);
        populateSUCAggregationTablesForQuery(eventId, timestamp, TEMP_SMS_CONTROLLER_CELL_SUM_SUC_DAY);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, "100090");
        final List<MssKPIRatioDrillTypeBSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioDrillTypeBSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio,
                impactedSubScribers);
    }

    @Test
    public void testTypeMSCDrilltypeBSC_Voice_Agg_15MIN() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(3);
        final String eventId = MSS_MS_ORIGINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_MS_ORIGINATING;
        final int noOfErrors = 2;
        final int noOfSuccess = 1;
        final int total = 3;
        final String successRatio = "33.33";
        final int impactedSubScribers = 2;

        creatVoiceAggregationTables_15MIN();
        createTopologyTables();

        populateERRAggregationTablesForQuery(eventId, timestamp, TEMP_VOICE_CONTROLLER_CELL_SUM_ERR_15MIN);
        populateERRAggregationTablesForQuery(eventId, timestamp, TEMP_VOICE_CONTROLLER_CELL_SUM_DROP_15MIN);
        populateSUCAggregationTablesForQuery(eventId, timestamp, TEMP_VOICE_CONTROLLER_CELL_SUM_SUC_15MIN);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, ONE_DAY);
        final List<MssKPIRatioDrillTypeBSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioDrillTypeBSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio,
                impactedSubScribers);
    }

    @Test
    public void testTypeMSCDrilltypeBSC_Voice_Agg_DAY() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        final String eventId = MSS_MS_ORIGINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_MS_ORIGINATING;
        final int noOfErrors = 2;
        final int noOfSuccess = 1;
        final int total = 3;
        final String successRatio = "33.33";
        final int impactedSubScribers = 2;

        createVoiceAggregationTables_DAY();
        createTopologyTables();

        populateERRAggregationTablesForQuery(eventId, timestamp, TEMP_VOICE_CONTROLLER_CELL_SUM_ERR_DAY);
        populateERRAggregationTablesForQuery(eventId, timestamp, TEMP_VOICE_CONTROLLER_CELL_SUM_DROP_DAY);
        populateSUCAggregationTablesForQuery(eventId, timestamp, TEMP_VOICE_CONTROLLER_CELL_SUM_SUC_DAY);
        populateTopologyTablesForQuery(eventType, eventId);

        final String json = getJsonString(eventId, "100090");
        final List<MssKPIRatioDrillTypeBSCResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioDrillTypeBSCResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, noOfErrors, noOfSuccess, total, successRatio,
                impactedSubScribers);
    }
}
