/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.mss.eventvolume;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.integritytests.mss.MssTestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.resources.mss.MssEventVolumeResource;
import com.ericsson.eniq.events.server.test.queryresults.mss.MssEventVolumeWithTypeResult;
import com.ericsson.eniq.events.server.test.sql.SQLCommand;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * 
 * @author echimma
 * @since 2011
 *
 */
public class MssEventVolumeWithPreparedTablesCELLTest extends
        MssTestsWithTemporaryTablesBaseTestCase<MssEventVolumeWithTypeResult> {
    private MssEventVolumeResource mssEventVolumeResource;

    private static final String DISPLAY_TYPE = CHART_PARAM;

    private static final String MAX_ROWS_VALUE = "50";

    private static final String DATA_START_RAW = "16082011";

    private static final String DATA_END_RAW = "16082011";

    private static final String TIME_START_RAW = "0900";

    private static final String TIME_END_RAW = "0930";

    private static final String TIMESTAMP_RAW = "2011-08-16 08:15:00";

    private static final String DATA_START_DAY = "01082011";

    private static final String DATA_END_DAY = "16082011";

    private static final String TIME_START_DAY = "0100";

    private static final String TIME_END_DAY = "0100";

    private static final String TIMESTAMP_DAY = "2011-08-03 23:00:00";

    private static final String DATA_START_15MIN = "15082011";

    private static final String DATA_END_15MIN = "15082011";

    private static final String TIME_START_15MIN = "0100";

    private static final String TIME_END_15MIN = "1400";

    private static final String TIMESTAMP_15MIN = "2011-08-15 01:15:00";

    private static final int JSON_SIZE_RAW = 30;

    private static final int JSON_SIZE_15MIN = 52;

    private static final int JSON_SIZE_DAY = 15;

    private static final int JSON_RESULT_POSTATION_RAW = 15;

    private static final int JSON_RESULT_POSTATION_15MIN = 5;

    private static final int JSON_RESULT_POSTATION_DAY = 2;

    private Connection conn1;

    private Connection conn2;

    private Connection conn3;

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        mssEventVolumeResource = new MssEventVolumeResource();
        mssEventVolumeResource.setQueryConstructor(this.queryConstructor);
        attachDependenciesForMSSBaseResource(mssEventVolumeResource);

        conn1 = getDWHDataSourceConnection();
        createTemporaryTableOnSpecificConnection(conn1, TEMP_GROUP_TYPE_E_TAC, groupColumns);
        interceptedDbConnectionManager.addConnection(conn1);

        conn2 = getDWHDataSourceConnection();
        createTemporaryTableOnSpecificConnection(conn2, TEMP_GROUP_TYPE_E_TAC, groupColumns);
        interceptedDbConnectionManager.addConnection(conn2);

        conn3 = getDWHDataSourceConnection();
        createTemporaryTableOnSpecificConnection(conn3, TEMP_GROUP_TYPE_E_TAC, groupColumns);
        interceptedDbConnectionManager.addConnection(conn3);
    }

    private void createRawTableAndPopulateRawData(final String timestamp) throws Exception {
        //1. Create raw tables for calculate Impacted IMSI
        createImpactedIMSITables(this.connection);

        //2. Create SMS Tables and populate data for test
        createSMSRawTablesAndPopulateData(conn1, TEMP_EVENT_E_MSS_SMS_CDR_SUC_RAW, TEMP_EVENT_E_MSS_SMS_CDR_ERR_RAW,
                timestamp);

        //3. Create Location Service Tables and populate data for test
        createLocRawTablesAndPopulateData(conn2, TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_SUC_RAW,
                TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_ERR_RAW, timestamp);

        //4. Create Voice Tables and populate data for test
        createVoiceRawTablesAndPopulateData(conn3, TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW,
                TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, timestamp);
    }

    private void createImpactedIMSITables(final Connection connection1) throws Exception {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(HIER321_ID);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_EVENT_E_MSS_SMS_CDR_SUC_RAW, columnsForRawTable);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_EVENT_E_MSS_SMS_CDR_ERR_RAW, columnsForRawTable);

        new SQLCommand(connection1).createTemporaryTableMss(TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_SUC_RAW,
                columnsForRawTable);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_ERR_RAW,
                columnsForRawTable);

        new SQLCommand(connection1).createTemporaryTableMss(TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW, columnsForRawTable);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, columnsForRawTable);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW,
                columnsForRawTable);
    }

    private void createAggTableAndPopulateAggData15MIN(final String timestamp) throws Exception {
        //1. Create raw tables for calculate Impacted IMSI
        createImpactedIMSITables(this.connection);

        //2. Create SMS Tables and populate data for test
        createSMSAggTablesAndPopulateData(conn1, TEMP_SMS_CONTROLLER_CELL_SUM_SUC_15MIN,
                TEMP_SMS_CONTROLLER_CELL_SUM_ERR_15MIN, timestamp);

        //3. Create Location Service Tables and populate data for test
        createLocAggTablesAndPopulateData(conn2, TEMP_LOC_CONTROLLER_CELL_SUM_SUC_15MIN,
                TEMP_LOC_CONTROLLER_CELL_SUM_ERR_15MIN, timestamp);

        //4. Create Voice Tables and populate data for test
        createVoiceAggTablesAndPopulateData(conn3, TEMP_VOICE_CONTROLLER_CELL_SUM_SUC_15MIN,
                TEMP_VOICE_CONTROLLER_CELL_SUM_ERR_15MIN, TEMP_VOICE_CONTROLLER_CELL_SUM_DROP_15MIN, timestamp);
    }

    private void createAggTableAndPopulateAggDataDAY(final String timestamp) throws Exception {
        //1. Create raw tables for calculate Impacted IMSI
        createImpactedIMSITables(this.connection);

        //2. Create SMS Tables and populate data for test
        createSMSAggTablesAndPopulateData(conn1, TEMP_SMS_CONTROLLER_CELL_SUM_SUC_DAY,
                TEMP_SMS_CONTROLLER_CELL_SUM_ERR_DAY, timestamp);

        //3. Create Location Service Tables and populate data for test
        createLocAggTablesAndPopulateData(conn2, TEMP_LOC_CONTROLLER_CELL_SUM_SUC_DAY,
                TEMP_LOC_CONTROLLER_CELL_SUM_ERR_DAY, timestamp);

        //4. Create Voice Tables and populate data for test
        createVoiceAggTablesAndPopulateData(conn3, TEMP_VOICE_CONTROLLER_CELL_SUM_SUC_DAY,
                TEMP_VOICE_CONTROLLER_CELL_SUM_ERR_DAY, TEMP_VOICE_CONTROLLER_CELL_SUM_DROP_DAY, timestamp);
    }

    private void createVoiceAggTablesAndPopulateData(final Connection connection1, final String sucTable,
            final String errTable, final String dropTable, final String timestamp) throws Exception {
        createVoiceAggregationTables(connection1, sucTable, errTable, dropTable);

        //for msOriginating ( EVENT_ID = 0 and (TELE_SERVICE_CODE != 18 or TELE_SERVICE_CODE is null) )
        populateAggVoiceData(connection1, MSS_MS_ORIGINATING_EVENT_ID, TEST_VALUE_TELE_SERVICE_CODE, timestamp,
                sucTable, errTable, dropTable);

        //For msOriginating_Emergency ( EVENT_ID = 0 and TELE_SERVICE_CODE = 18 )
        populateAggVoiceData(connection1, MSS_MS_ORIGINATING_EVENT_ID, 18, timestamp, sucTable, errTable, dropTable);

        //MSTerminating EVENT_ID = 1 
        populateAggVoiceData(connection1, MSS_MS_TERMINATING_EVENT_ID, TEST_VALUE_TELE_SERVICE_CODE, timestamp,
                sucTable, errTable, dropTable);

        //msCallForwarding EVENT_ID = 2
        populateAggVoiceData(connection1, MSS_CALL_FORWARDING_EVENT_ID, TEST_VALUE_TELE_SERVICE_CODE, timestamp,
                sucTable, errTable, dropTable);

        //msRoamingCall EVENT_ID = 3
        populateAggVoiceData(connection1, MSS_ROAMING_CALL_EVENT_ID, TEST_VALUE_TELE_SERVICE_CODE, timestamp, sucTable,
                errTable, dropTable);
    }

    private void createVoiceRawTablesAndPopulateData(final Connection connection1, final String sucTable,
            final String errTable, final String dropTable, final String timestamp) throws Exception {
        createVoiceRawTables(connection1, sucTable, errTable, dropTable);

        //for msOriginating ( EVENT_ID = 0 and (TELE_SERVICE_CODE != 18 or TELE_SERVICE_CODE is null) )
        populateRawVoiceData(connection1, MSS_MS_ORIGINATING_EVENT_ID, TEST_VALUE_TELE_SERVICE_CODE, timestamp,
                sucTable, errTable, dropTable);

        //For msOriginating_Emergency ( EVENT_ID = 0 and TELE_SERVICE_CODE = 18 )
        populateRawVoiceData(connection1, MSS_MS_ORIGINATING_EVENT_ID, 18, timestamp, sucTable, errTable, dropTable);

        //MSTerminating EVENT_ID = 1 
        populateRawVoiceData(connection1, MSS_MS_TERMINATING_EVENT_ID, TEST_VALUE_TELE_SERVICE_CODE, timestamp,
                sucTable, errTable, dropTable);

        //msCallForwarding EVENT_ID = 2
        populateRawVoiceData(connection1, MSS_CALL_FORWARDING_EVENT_ID, TEST_VALUE_TELE_SERVICE_CODE, timestamp,
                sucTable, errTable, dropTable);

        //msRoamingCall EVENT_ID = 3
        populateRawVoiceData(connection1, MSS_ROAMING_CALL_EVENT_ID, TEST_VALUE_TELE_SERVICE_CODE, timestamp, sucTable,
                errTable, dropTable);
    }

    private void createSMSRawTablesAndPopulateData(final Connection connection1, final String sucTable,
            final String errTable, final String timestamp) throws Exception {
        createLocAndSmsRawTables(connection1, sucTable, errTable);

        //for SMS test, EVENT_ID = 4, MSORIGINATING_SMS
        populateRawSMSOrLOCData(connection1, MSS_SMS_MS_ORIGINATING_EVENT_ID, timestamp, sucTable);
        populateRawSMSOrLOCData(connection1, MSS_SMS_MS_ORIGINATING_EVENT_ID, timestamp, errTable);
        //for SMS test EVENT_ID = 5, MSTERMINATING_SMS
        populateRawSMSOrLOCData(connection1, MSS_SMS_MS_TERMINATING_EVENT_ID, timestamp, sucTable);
        populateRawSMSOrLOCData(connection1, MSS_SMS_MS_TERMINATING_EVENT_ID, timestamp, errTable);
    }

    private void createSMSAggTablesAndPopulateData(final Connection connection1, final String sucTable,
            final String errTable, final String timestamp) throws Exception {
        createLocAndSmsAggTables(connection1, sucTable, errTable);

        //for SMS test, EVENT_ID = 4, MSORIGINATING_SMS
        populateAggSMSOrLOCDataSuc(connection1, MSS_SMS_MS_ORIGINATING_EVENT_ID, timestamp, sucTable);
        populateAggSMSOrLOCDataErr(connection1, MSS_SMS_MS_ORIGINATING_EVENT_ID, timestamp, errTable);
        //for SMS test EVENT_ID = 5, MSTERMINATING_SMS
        populateAggSMSOrLOCDataSuc(connection1, MSS_SMS_MS_TERMINATING_EVENT_ID, timestamp, sucTable);
        populateAggSMSOrLOCDataErr(connection1, MSS_SMS_MS_TERMINATING_EVENT_ID, timestamp, errTable);
    }

    private void createLocRawTablesAndPopulateData(final Connection connection1, final String sucTable,
            final String errTable, final String timestamp) throws Exception {
        createLocAndSmsRawTables(connection1, sucTable, errTable);
        //for Location Service test, EVENT_ID = 6
        populateRawSMSOrLOCData(connection1, MSS_LOCATION_SERVICE_EVENT_ID, timestamp, sucTable);
        populateRawSMSOrLOCData(connection1, MSS_LOCATION_SERVICE_EVENT_ID, timestamp, errTable);
    }

    private void createLocAggTablesAndPopulateData(final Connection connection1, final String sucTable,
            final String errTable, final String timestamp) throws Exception {
        createLocAndSmsAggTables(connection1, sucTable, errTable);
        //for Location Service test, EVENT_ID = 6
        populateAggSMSOrLOCDataSuc(connection1, MSS_LOCATION_SERVICE_EVENT_ID, timestamp, sucTable);
        populateAggSMSOrLOCDataErr(connection1, MSS_LOCATION_SERVICE_EVENT_ID, timestamp, errTable);
    }

    private void createVoiceRawTables(final Connection connection1, final String sucTable, final String errTable,
            final String dropTable) throws Exception {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(HIER321_ID);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(TELE_SERVICE_CODE);
        new SQLCommand(connection1).createTemporaryTableMss(sucTable, columnsForRawTable);
        new SQLCommand(connection1).createTemporaryTableMss(errTable, columnsForRawTable);
        new SQLCommand(connection1).createTemporaryTableMss(dropTable, columnsForRawTable);
    }

    private void createLocAndSmsRawTables(final Connection connection1, final String sucTable, final String errTable)
            throws Exception {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(HIER321_ID);
        columnsForRawTable.add(TAC);
        new SQLCommand(connection1).createTemporaryTableMss(sucTable, columnsForRawTable);
        new SQLCommand(connection1).createTemporaryTableMss(errTable, columnsForRawTable);
    }

    private void createLocAndSmsAggTables(final Connection connection1, final String sucTable, final String errTable)
            throws Exception {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.clear();
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(HIER321_ID);
        columnsForRawTable.add(NO_OF_ERRORS);
        columnsForRawTable.add(NO_OF_SUCCESSES);
        new SQLCommand(connection1).createTemporaryTableMss(sucTable, columnsForRawTable);
        new SQLCommand(connection1).createTemporaryTableMss(errTable, columnsForRawTable);
    }

    private void createVoiceAggregationTables(final Connection connection1, final String sucTable,
            final String errTable, final String dropTabl) throws Exception {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(HIER321_ID);
        columnsForRawTable.add(NO_OF_ERRORS);
        columnsForRawTable.add(NO_OF_SUCCESSES);
        columnsForRawTable.add(TELE_SERVICE_CODE);
        new SQLCommand(connection1).createTemporaryTableMss(sucTable, columnsForRawTable);
        new SQLCommand(connection1).createTemporaryTableMss(errTable, columnsForRawTable);
        new SQLCommand(connection1).createTemporaryTableMss(dropTabl, columnsForRawTable);
    }

    private void populateAggVoiceData(final Connection connection1, final String eventId, final int teleServiceCode,
            final String timestamp, final String sucTable, final String errTable, final String dropTable)
            throws SQLException {
        populateAggVoiceDataSuc(connection1, eventId, teleServiceCode, timestamp, sucTable);
        populateAggVoiceDataErr(connection1, eventId, teleServiceCode, timestamp, errTable);
        populateAggVoiceDataErr(connection1, eventId, teleServiceCode, timestamp, dropTable);
    }

    private void populateRawVoiceData(final Connection connection1, final String eventId, final int teleServiceCode,
            final String timestamp, final String sucTable, final String errTable, final String dropTable)
            throws SQLException {
        populateRawVoiceData(connection1, eventId, teleServiceCode, timestamp, sucTable);
        populateRawVoiceData(connection1, eventId, teleServiceCode, timestamp, errTable);
        populateRawVoiceData(connection1, eventId, teleServiceCode, timestamp, dropTable);
    }

    private void populateRawVoiceData(final Connection connection1, final String eventId, final int teleServiceCode,
            final String timestamp, final String tableName) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(TAC, SAMPLE_TAC);
        values.put(DATETIME_ID, timestamp);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        values.put(TELE_SERVICE_CODE, teleServiceCode);

        new SQLCommand(connection1).insertRow(tableName, values);
    }

    private void populateAggVoiceDataErr(final Connection connection1, final String eventId, final int teleServiceCode,
            final String timestamp, final String tableName) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(NO_OF_ERRORS, 1);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(DATETIME_ID, timestamp);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        values.put(TELE_SERVICE_CODE, teleServiceCode);

        new SQLCommand(connection1).insertRow(tableName, values);
    }

    private void populateAggVoiceDataSuc(final Connection connection1, final String eventId, final int teleServiceCode,
            final String timestamp, final String tableName) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(NO_OF_ERRORS, 0);
        values.put(NO_OF_SUCCESSES, 1);
        values.put(DATETIME_ID, timestamp);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        values.put(TELE_SERVICE_CODE, teleServiceCode);

        new SQLCommand(connection1).insertRow(tableName, values);
    }

    private void populateAggSMSOrLOCDataErr(final Connection connection1, final String eventId, final String timestamp,
            final String tableName) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(NO_OF_ERRORS, 1);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        values.put(DATETIME_ID, timestamp);

        new SQLCommand(connection1).insertRow(tableName, values);
    }

    private void populateAggSMSOrLOCDataSuc(final Connection connection1, final String eventId, final String timestamp,
            final String tableName) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(NO_OF_ERRORS, 0);
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        values.put(DATETIME_ID, timestamp);

        new SQLCommand(connection1).insertRow(tableName, values);
    }

    private void populateRawSMSOrLOCData(final Connection connection1, final String eventId, final String timestamp,
            final String tableName) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(TAC, SAMPLE_TAC);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        values.put(DATETIME_ID, timestamp);

        new SQLCommand(connection1).insertRow(tableName, values);
    }

    private String getJsonString(final String dataFrom, final String dataTo, final String timefrom, final String timeTo)
            throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(DATE_FROM_QUERY_PARAM, dataFrom);
        map.putSingle(DATE_TO_QUERY_PARAM, dataTo);
        map.putSingle(TIME_FROM_QUERY_PARAM, timefrom);
        map.putSingle(TIME_TO_QUERY_PARAM, timeTo);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(NODE_PARAM, TEST_VALUE_MSS_CELL_NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfoMss(map, mssEventVolumeResource);
        return mssEventVolumeResource.getData();
    }

    private void validateResultFromTablesRaw(final List<MssEventVolumeWithTypeResult> results) {
        assertThat(results.size(), is(JSON_SIZE_RAW));
        final MssEventVolumeWithTypeResult result = results.get(JSON_RESULT_POSTATION_RAW);

        assertThat(result.getMSOriginatingCallCompletionCount(), is(1));//2
        assertThat(result.getMSOriginatingCallBlockCount(), is(1));//3
        assertThat(result.getMSOriginatingCallDropCount(), is(1));//4
        assertThat(result.getMSOriginatingEmergencyCallCompletionCount(), is(1));//5
        assertThat(result.getMSOriginatingEmergencyCallBlockCount(), is(1));//6
        assertThat(result.getMSOriginatingEmergencyCallDropCount(), is(1));//7
        assertThat(result.getMSTerminatingCallCompletionCount(), is(1));//8
        assertThat(result.getMSTerminatingCallBlockCount(), is(1));//9
        assertThat(result.getMSTerminatingCallDropCount(), is(1));//10
        assertThat(result.getCallForwardingCallCount(), is(1));//11
        assertThat(result.getCallForwardingCallBlockCount(), is(1));//12
        assertThat(result.getCallForwardingCallDropCount(), is(1));//13
        assertThat(result.getRoamingCallForwardingCallCount(), is(1));//14
        assertThat(result.getRoamingCallForwardingCallBlockCount(), is(1));//15
        assertThat(result.getRoamingCallForwardingCallDropCount(), is(1));//16
        assertThat(result.getLocationRequestsCount(), is(1));//17
        assertThat(result.getUnsuccessfulLocationRequestCount(), is(1));//18
        assertThat(result.getMSOriginatingSMSCount(), is(1));//19
        assertThat(result.getMSOriginatingSMSFailCount(), is(1));//20
        assertThat(result.getMSTerminatingSMSCount(), is(1));//21
        assertThat(result.getMSTerminatingSMSFailCount(), is(1));//21
        assertThat(result.getImpactedSubscribers(), is(0));
        assertThat(result.getTotalNetworkEvents(), is(21));//22
    }

    private void validateResultFromTables15Min(final List<MssEventVolumeWithTypeResult> results) {
        assertThat(results.size(), is(JSON_SIZE_15MIN));
        final MssEventVolumeWithTypeResult result = results.get(JSON_RESULT_POSTATION_15MIN);

        assertThat(result.getMSOriginatingCallCompletionCount(), is(1));//2
        assertThat(result.getMSOriginatingCallBlockCount(), is(1));//3
        assertThat(result.getMSOriginatingCallDropCount(), is(1));//4
        assertThat(result.getMSOriginatingEmergencyCallCompletionCount(), is(1));//5
        assertThat(result.getMSOriginatingEmergencyCallBlockCount(), is(1));//6
        assertThat(result.getMSOriginatingEmergencyCallDropCount(), is(1));//7
        assertThat(result.getMSTerminatingCallCompletionCount(), is(1));//8
        assertThat(result.getMSTerminatingCallBlockCount(), is(1));//9
        assertThat(result.getMSTerminatingCallDropCount(), is(1));//10
        assertThat(result.getCallForwardingCallCount(), is(1));//11
        assertThat(result.getCallForwardingCallBlockCount(), is(1));//12
        assertThat(result.getCallForwardingCallDropCount(), is(1));//13
        assertThat(result.getRoamingCallForwardingCallCount(), is(1));//14
        assertThat(result.getRoamingCallForwardingCallBlockCount(), is(1));//15
        assertThat(result.getRoamingCallForwardingCallDropCount(), is(1));//16
        assertThat(result.getLocationRequestsCount(), is(1));//17
        assertThat(result.getUnsuccessfulLocationRequestCount(), is(1));//18
        assertThat(result.getMSOriginatingSMSCount(), is(1));//19
        assertThat(result.getMSOriginatingSMSFailCount(), is(1));//20
        assertThat(result.getMSTerminatingSMSCount(), is(1));//21
        assertThat(result.getMSTerminatingSMSFailCount(), is(1));//21
        assertThat(result.getImpactedSubscribers(), is(0));
        assertThat(result.getTotalNetworkEvents(), is(21));//22
    }

    private void validateResultFromTablesDay(final List<MssEventVolumeWithTypeResult> results) {
        assertThat(results.size(), is(JSON_SIZE_DAY));
        final MssEventVolumeWithTypeResult result = results.get(JSON_RESULT_POSTATION_DAY);

        assertThat(result.getMSOriginatingCallCompletionCount(), is(1));//2
        assertThat(result.getMSOriginatingCallBlockCount(), is(1));//3
        assertThat(result.getMSOriginatingCallDropCount(), is(1));//4
        assertThat(result.getMSOriginatingEmergencyCallCompletionCount(), is(1));//5
        assertThat(result.getMSOriginatingEmergencyCallBlockCount(), is(1));//6
        assertThat(result.getMSOriginatingEmergencyCallDropCount(), is(1));//7
        assertThat(result.getMSTerminatingCallCompletionCount(), is(1));//8
        assertThat(result.getMSTerminatingCallBlockCount(), is(1));//9
        assertThat(result.getMSTerminatingCallDropCount(), is(1));//10
        assertThat(result.getCallForwardingCallCount(), is(1));//11
        assertThat(result.getCallForwardingCallBlockCount(), is(1));//12
        assertThat(result.getCallForwardingCallDropCount(), is(1));//13
        assertThat(result.getRoamingCallForwardingCallCount(), is(1));//14
        assertThat(result.getRoamingCallForwardingCallBlockCount(), is(1));//15
        assertThat(result.getRoamingCallForwardingCallDropCount(), is(1));//16
        assertThat(result.getLocationRequestsCount(), is(1));//17
        assertThat(result.getUnsuccessfulLocationRequestCount(), is(1));//18
        assertThat(result.getMSOriginatingSMSCount(), is(1));//19
        assertThat(result.getMSOriginatingSMSFailCount(), is(1));//20
        assertThat(result.getMSTerminatingSMSCount(), is(1));//21
        assertThat(result.getMSTerminatingSMSFailCount(), is(1));//21
        assertThat(result.getImpactedSubscribers(), is(0));
        assertThat(result.getTotalNetworkEvents(), is(21));//22
    }

    @Test
    public void testGetEventVolumeDataTypeCELLRaw() throws Exception {
        createRawTableAndPopulateRawData(TIMESTAMP_RAW);

        final String json = getJsonString(DATA_START_RAW, DATA_END_RAW, TIME_START_RAW, TIME_END_RAW);

        final List<MssEventVolumeWithTypeResult> summaryResult = getTranslator().translateResult(json,
                MssEventVolumeWithTypeResult.class);

        validateResultFromTablesRaw(summaryResult);
    }

    @Test
    public void testGetEventVolumeDataTypeCELL15MIN() throws Exception {
        createAggTableAndPopulateAggData15MIN(TIMESTAMP_15MIN);

        final String json = getJsonString(DATA_START_15MIN, DATA_END_15MIN, TIME_START_15MIN, TIME_END_15MIN);

        final List<MssEventVolumeWithTypeResult> summaryResult = getTranslator().translateResult(json,
                MssEventVolumeWithTypeResult.class);

        validateResultFromTables15Min(summaryResult);
    }

    @Test
    public void testGetEventVolumeDataTypeCELLDAY() throws Exception {
        createAggTableAndPopulateAggDataDAY(TIMESTAMP_DAY);

        final String json = getJsonString(DATA_START_DAY, DATA_END_DAY, TIME_START_DAY, TIME_END_DAY);

        final List<MssEventVolumeWithTypeResult> summaryResult = getTranslator().translateResult(json,
                MssEventVolumeWithTypeResult.class);

        validateResultFromTablesDay(summaryResult);
    }
}
