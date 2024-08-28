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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SimpleTimeZone;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.integritytests.mss.MssTestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.resources.mss.MssKPIRatioResource;
import com.ericsson.eniq.events.server.test.queryresults.MssEventAnalysisDetailedResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * 
 * @author echimma
 * @since 2011
 *
 */
public class MssKPIRatioTypeMSCDrillByEventsWithPreparedTablesTest extends
        MssTestsWithTemporaryTablesBaseTestCase<MssEventAnalysisDetailedResult> {
    private MssKPIRatioResource mssKPIRatioResource;

    /**
     * 
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        mssKPIRatioResource = new MssKPIRatioResource();
        attachDependenciesForMSSBaseResource(mssKPIRatioResource);
    }

    /**
     * 
     * @return
     */
    private final Collection<String> getVoiceRawColumns() {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(EVENT_TIME);
        columnsForRawTable.add(CALLING_PARTY_NUM);
        columnsForRawTable.add(MSISDN);
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(INTERNAL_CAUSE_CODE);
        columnsForRawTable.add(FAULT_CODE);
        columnsForRawTable.add(RAT);
        columnsForRawTable.add(VENDOR);
        columnsForRawTable.add(INTERNAL_LOCATION_CODE);
        columnsForRawTable.add(BEARER_SERVICE_CODE);
        columnsForRawTable.add(TELE_SERVICE_CODE);
        columnsForRawTable.add(OSS_ID);
        columnsForRawTable.add(CALL_ID_NUM);
        columnsForRawTable.add(TYPE_OF_CALLING_SUB);
        columnsForRawTable.add(CALLED_PARTY_NUM);
        columnsForRawTable.add(CALLING_SUB_IMSI);
        columnsForRawTable.add(CALLING_SUB_IMEI);
        columnsForRawTable.add(CALLED_SUB_IMSI);
        columnsForRawTable.add(CALLED_SUB_IMEI);
        columnsForRawTable.add(MS_ROAMING_NUM);
        columnsForRawTable.add(DISCONNECT_PARTY);
        columnsForRawTable.add(CALL_DURATION);
        columnsForRawTable.add(SEIZURE_TIME);
        columnsForRawTable.add(ORIGINAL_CALLED_NUM);
        columnsForRawTable.add(REDIRECT_NUM);
        columnsForRawTable.add(REDIRECT_COUNTER);
        columnsForRawTable.add(REDIRECT_IMSI);
        columnsForRawTable.add(REDIRECT_SPN);
        columnsForRawTable.add(CALL_POSITION);
        columnsForRawTable.add(EOS_INFO);
        columnsForRawTable.add(RECORD_SEQ_NUM);
        columnsForRawTable.add(MCC);
        columnsForRawTable.add(MNC);
        columnsForRawTable.add(RAC);
        columnsForRawTable.add(LAC);
        columnsForRawTable.add(EVNTSRC_ID);
        columnsForRawTable.add(HIER3_ID);
        columnsForRawTable.add(HIER321_ID);
        columnsForRawTable.add(ACCESS_AREA_CODE);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(NETWORK_CALL_REFERENCE);
        columnsForRawTable.add(EXTERNAL_CAUSE_CODE);
        columnsForRawTable.add(EXTERNAL_PROTOCOL_ID);
        columnsForRawTable.add(INCOMING_ROUTE);
        columnsForRawTable.add(OUTGOING_ROUTE);
        return columnsForRawTable;
    }

    /**
     * 
     * @return
     */
    private final Collection<String> getLocRawColumns() {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(EVNTSRC_ID);
        columnsForRawTable.add(HIER3_ID);
        columnsForRawTable.add(HIER321_ID);
        columnsForRawTable.add(RAT);
        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add("DATE_ID");
        columnsForRawTable.add(HOUR_ID);
        columnsForRawTable.add(LCS_CLIENT_TYPE);
        columnsForRawTable.add(UNSUC_POSITION_REASON);
        columnsForRawTable.add(TYPE_LOCATION_REQ);
        columnsForRawTable.add(EVENT_TIME);
        columnsForRawTable.add(MSISDN);
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(CALL_ID_NUM);
        columnsForRawTable.add(TARGET_MSISDN);
        columnsForRawTable.add(TARGET_IMSI);
        columnsForRawTable.add(TARGET_IMEI);
        columnsForRawTable.add(LCS_CLIENT_ID);
        columnsForRawTable.add(POSITION_DELIVERY);
        columnsForRawTable.add(RECORD_SEQ_NUM);
        columnsForRawTable.add(NETWORK_CALL_REFERENCE);
        columnsForRawTable.add(MCC);
        columnsForRawTable.add(MNC);
        columnsForRawTable.add(RAC);
        columnsForRawTable.add(LAC);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(EXTERNAL_PROTOCOL_ID);

        return columnsForRawTable;
    }

    /**
     * 
     * @return
     */
    private final Collection<String> getSMSRawColumns() {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(EVNTSRC_ID);
        columnsForRawTable.add(HIER3_ID);
        columnsForRawTable.add(HIER321_ID);
        columnsForRawTable.add(RAT);
        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(DATE_ID);
        columnsForRawTable.add(HOUR_ID);
        columnsForRawTable.add(SMS_RESULT);
        columnsForRawTable.add(MSG_TYPE_INDICATOR);
        columnsForRawTable.add(EVENT_TIME);
        columnsForRawTable.add(MSISDN);
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(BEARER_SERVICE_CODE);
        columnsForRawTable.add(TELE_SERVICE_CODE);
        columnsForRawTable.add(CALL_ID_NUM);
        columnsForRawTable.add(TYPE_OF_CALLING_SUB);
        columnsForRawTable.add(CALLING_PARTY_NUM);
        columnsForRawTable.add(CALLED_PARTY_NUM);
        columnsForRawTable.add(CALLING_SUB_IMSI);
        columnsForRawTable.add(CALLED_SUB_IMSI);
        columnsForRawTable.add(CALLING_SUB_IMEI);
        columnsForRawTable.add(CALLED_SUB_IMEI);
        columnsForRawTable.add(CALLING_SUB_IMEISV);
        columnsForRawTable.add(CALLED_SUB_IMEISV);
        columnsForRawTable.add(ORIGINATING_NUM);
        columnsForRawTable.add(DEST_NUM);
        columnsForRawTable.add(SERVICE_CENTRE);
        columnsForRawTable.add(ORIGINATING_TIME);
        columnsForRawTable.add(DELIVERY_TIME);
        columnsForRawTable.add(RECORD_SEQ_NUM);
        columnsForRawTable.add(MCC);
        columnsForRawTable.add(MNC);
        columnsForRawTable.add(RAC);
        columnsForRawTable.add(LAC);
        columnsForRawTable.add(DATETIME_ID);

        return columnsForRawTable;
    }

    /**
     * 
     * @throws Exception
     */
    private void createVoiceRawTables() throws Exception {
        final Collection<String> columnsForRawTable = getVoiceRawColumns();

        createTemporaryTableMss(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, columnsForRawTable);
        createTemporaryTableMss(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, columnsForRawTable);
        createTemporaryTableMss(TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW, columnsForRawTable);
    }

    /**
     * @throws Exception 
     * 
     */
    private void createLocRawTables() throws Exception {
        final Collection<String> columnsForRawTable = getLocRawColumns();

        createTemporaryTableMss(TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_ERR_RAW, columnsForRawTable);
        createTemporaryTableMss(TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_SUC_RAW, columnsForRawTable);
    }

    /**
     * @throws Exception 
     * 
     */
    private void createSMSRawTables() throws Exception {
        final Collection<String> columnsForRawTable = getSMSRawColumns();

        createTemporaryTableMss(TEMP_EVENT_E_MSS_SMS_CDR_ERR_RAW, columnsForRawTable);
        createTemporaryTableMss(TEMP_EVENT_E_MSS_SMS_CDR_SUC_RAW, columnsForRawTable);
    }

    /**
     * 
     * @throws Exception
     */
    private void createVoiceTopologyTables() throws Exception {
        final Collection<String> columnsForTopologyTable = new ArrayList<String>();
        columnsForTopologyTable.add(EVENT_ID);
        columnsForTopologyTable.add("EVENT_ID_DESC");
        createTemporaryTableMss(TEMP_DIM_E_MSS_EVENTTYPE, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(FAULT_CODE);
        columnsForTopologyTable.add(FAULT_CODE_DESC);
        columnsForTopologyTable.add(ADVICE);
        createTemporaryTableMss(TEMP_DIM_E_MSS_FAULT_CODE, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(INTERNAL_CAUSE_CODE);
        columnsForTopologyTable.add(INTERNAL_CAUSE_CODE_DESC);
        createTemporaryTableMss(TEMP_DIM_E_MSS_INTERNAL_CAUSE_CODE, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(VENDOR);
        columnsForTopologyTable.add(HIERARCHY_1);
        columnsForTopologyTable.add(HIERARCHY_3);
        columnsForTopologyTable.add(HIER3_ID);
        columnsForTopologyTable.add(HIER321_ID);

        createTemporaryTableMss(TEMP_DIM_E_SGEH_HIER321, columnsForTopologyTable);
        createTemporaryTableMss(TEMP_DIM_Z_SGEH_HIER321, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(TAC);
        columnsForTopologyTable.add(MANUFACTURER);
        columnsForTopologyTable.add(MARKETING_NAME);
        createTemporaryTableMss(TEMP_DIM_E_SGEH_TAC, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(EVNTSRC_ID);
        columnsForTopologyTable.add(EVENT_SOURCE_NAME);
        createTemporaryTableMss(TEMP_DIM_E_MSS_EVNTSRC, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(RAT);
        columnsForTopologyTable.add(RAT_DESC);
        createTemporaryTableMss(TEMP_DIM_E_SGEH_RAT, columnsForTopologyTable);

    }

    private void createLocTopologyTables() throws Exception {
        final Collection<String> columnsForTopologyTable = new ArrayList<String>();

        columnsForTopologyTable.add(VENDOR);
        columnsForTopologyTable.add(HIERARCHY_1);
        columnsForTopologyTable.add(HIERARCHY_3);
        columnsForTopologyTable.add(HIER3_ID);
        columnsForTopologyTable.add(HIER321_ID);

        createTemporaryTableMss(TEMP_DIM_E_SGEH_HIER321, columnsForTopologyTable);
        createTemporaryTableMss(TEMP_DIM_Z_SGEH_HIER321, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(EVENT_ID);
        columnsForTopologyTable.add("EVENT_ID_DESC");
        createTemporaryTableMss(TEMP_DIM_E_MSS_EVENTTYPE, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(EVNTSRC_ID);
        columnsForTopologyTable.add(EVENT_SOURCE_NAME);
        createTemporaryTableMss(TEMP_DIM_E_MSS_EVNTSRC, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(RAT);
        columnsForTopologyTable.add(RAT_DESC);
        createTemporaryTableMss(TEMP_DIM_E_SGEH_RAT, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(TAC);
        columnsForTopologyTable.add(MANUFACTURER);
        columnsForTopologyTable.add(MARKETING_NAME);
        createTemporaryTableMss(TEMP_DIM_E_SGEH_TAC, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(LCS_CLIENT_TYPE);
        columnsForTopologyTable.add(LCS_CLIENT_TYPE_DESC);
        createTemporaryTableMss(TEMP_DIM_E_MSS_LCS_CLIENT_TYPE, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(TYPE_LOCATION_REQ);
        columnsForTopologyTable.add(TYPE_LOCATION_REQ_DESC);
        createTemporaryTableMss(TEMP_DIM_E_MSS_TYPE_LOCATION_REQ, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(UNSUC_POSITION_REASON);
        columnsForTopologyTable.add(UNSUC_POSITION_REASON_DESC);
        createTemporaryTableMss(TEMP_DIM_E_MSS_UNSUC_POSITION_REASON, columnsForTopologyTable);
    }

    private void createSMSTopologyTables() throws Exception {
        final Collection<String> columnsForTopologyTable = new ArrayList<String>();

        columnsForTopologyTable.add(VENDOR);
        columnsForTopologyTable.add(HIERARCHY_1);
        columnsForTopologyTable.add(HIERARCHY_3);
        columnsForTopologyTable.add(HIER3_ID);
        columnsForTopologyTable.add(HIER321_ID);

        createTemporaryTableMss(TEMP_DIM_E_SGEH_HIER321, columnsForTopologyTable);
        createTemporaryTableMss(TEMP_DIM_Z_SGEH_HIER321, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(EVENT_ID);
        columnsForTopologyTable.add("EVENT_ID_DESC");
        createTemporaryTableMss(TEMP_DIM_E_MSS_EVENTTYPE, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(EVNTSRC_ID);
        columnsForTopologyTable.add(EVENT_SOURCE_NAME);
        createTemporaryTableMss(TEMP_DIM_E_MSS_EVNTSRC, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(RAT);
        columnsForTopologyTable.add(RAT_DESC);
        createTemporaryTableMss(TEMP_DIM_E_SGEH_RAT, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(TAC);
        columnsForTopologyTable.add(MANUFACTURER);
        columnsForTopologyTable.add(MARKETING_NAME);
        createTemporaryTableMss(TEMP_DIM_E_SGEH_TAC, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(MSG_TYPE_INDICATOR);
        columnsForTopologyTable.add(MSG_TYPE_INDICATOR_DESC);
        createTemporaryTableMss(TEMP_DIM_E_MSS_MSG_TYPE_INDICATOR, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(SMS_RESULT);
        columnsForTopologyTable.add(SMS_RESULT_DESC);
        createTemporaryTableMss(TEMP_DIM_E_MSS_SMS_RESULT, columnsForTopologyTable);
    }

    /**
     * 
     * @param eventId
     * @param timestamp
     * @param tableName
     * @throws SQLException
     */
    private void populateVoiceRawTablesForQuery(final String eventId, final String timestamp, final String tableName)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(EVENT_TIME, timestamp);
        values.put(CALLING_PARTY_NUM, TEST_VALUE_CALLING_PARTY_NUM);
        values.put(MSISDN, TEST_VALUE_MSISDN);
        values.put(IMSI, TEST_VALUE_IMSI);
        values.put(TAC, SAMPLE_TAC);
        values.put(INTERNAL_CAUSE_CODE, TEST_VALUE_INTERNAL_CAUSE_CODE);
        values.put(FAULT_CODE, TEST_VALUE_FAULT_CODE);
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR, ERICSSON);
        values.put(INTERNAL_LOCATION_CODE, TEST_VALUE_INTERNAL_LOCATION_CODE);
        values.put(BEARER_SERVICE_CODE, TEST_VALUE_BEARER_SERVICE_CODE);
        values.put(TELE_SERVICE_CODE, TEST_VALUE_TELE_SERVICE_CODE);
        values.put(OSS_ID, TEST_VALUE_OSS_ID);
        values.put(CALL_ID_NUM, TEST_VALUE_CALL_ID_NUM);
        values.put(TYPE_OF_CALLING_SUB, TEST_VALUE_TYPE_OF_CALLING_SUB);
        values.put(CALLED_PARTY_NUM, TEST_VALUE_CALLED_PARTY_NUM);
        values.put(CALLING_SUB_IMSI, TEST_VALUE_CALLING_SUB_IMSI);
        values.put(CALLING_SUB_IMEI, TEST_VALUE_CALLING_SUB_IMEI);
        values.put(CALLED_SUB_IMSI, TEST_VALUE_CALLED_SUB_IMSI);
        values.put(CALLED_SUB_IMEI, TEST_VALUE_CALLED_SUB_IMEI);
        values.put(MS_ROAMING_NUM, TEST_VALUE_MS_ROAMING_NUM);
        values.put(DISCONNECT_PARTY, TEST_VALUE_DISCONNECT_PARTY);
        values.put(CALL_DURATION, TEST_VALUE_CALL_DURATION);
        values.put(SEIZURE_TIME, TEST_VALUE_SEIZURE_TIME);
        values.put(ORIGINAL_CALLED_NUM, TEST_VALUE_ORIGINAL_CALLED_NUM);
        values.put(REDIRECT_NUM, TEST_VALUE_REDIRECT_NUM);
        values.put(REDIRECT_COUNTER, TEST_VALUE_REDIRECT_COUNTER);
        values.put(REDIRECT_IMSI, TEST_VALUE_REDIRECT_IMSI);
        values.put(REDIRECT_SPN, TEST_VALUE_REDIRECT_SPN);
        values.put(CALL_POSITION, TEST_VALUE_CALL_POSITION);
        values.put(EOS_INFO, TEST_VALUE_EOS_INFO);
        values.put(RECORD_SEQ_NUM, TEST_VALUE_RECORD_SEQ_NUM);
        values.put(MCC, TEST_VALUE_MCC);
        values.put(MNC, TEST_VALUE_MNC);
        values.put(RAC, TEST_VALUE_RAC);
        values.put(LAC, TEST_VALUE_LAC);
        values.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        values.put(HIER3_ID, TEST_VALUE_MSS_HIER3_ID);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        values.put(ACCESS_AREA_CODE, TEST_VALUE_MSS_ACCESS_AREA_CODE);
        values.put(NETWORK_CALL_REFERENCE, TEST_VALUE_NETWORK_CALL_REFERENCE);
        values.put(DATETIME_ID, timestamp);
        values.put(INCOMING_ROUTE, TEST_VALUE_INCOMING_ROUTE);
        values.put(OUTGOING_ROUTE, TEST_VALUE_OUTGOING_ROUTE);
        values.put(EXTERNAL_CAUSE_CODE, TEST_VALUE_EXTERNAL_CAUSE_CODE);
        values.put(EXTERNAL_PROTOCOL_ID, TEST_VALUE_EXTERNAL_PROTOCOL_ID);

        insertRow(tableName, values);
    }

    /**
     * 
     * @param eventId
     * @param timestamp
     * @param tableName
     * @throws SQLException
     * @throws ParseException 
     */
    private void populateLocRawTablesForQuery(final String eventId, final String timestamp, final String tableName)
            throws SQLException, ParseException {

        final Map<String, Object> values = new HashMap<String, Object>();
        final String dateId = getDateId(timestamp);
        final int hourId = getHourId(timestamp);
        values.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        values.put(HIER3_ID, TEST_VALUE_MSS_HIER3_ID);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        values.put(RAT, RAT_FOR_GSM);
        values.put(EVENT_ID, eventId);
        values.put(DATE_ID, dateId);
        values.put(HOUR_ID, hourId);
        values.put(LCS_CLIENT_TYPE, TEST_VALUE_LCS_CLIENT_TYPE);
        values.put(UNSUC_POSITION_REASON, TEST_VALUE_UNSUC_POSITION_REASON);
        values.put(TYPE_LOCATION_REQ, TEST_VALUE_TYPE_LOCATION_REQ);
        values.put(EVENT_TIME, timestamp);
        values.put(MSISDN, TEST_VALUE_MSISDN);
        values.put(IMSI, TEST_VALUE_IMSI);
        values.put(TAC, SAMPLE_TAC);
        values.put(CALL_ID_NUM, TEST_VALUE_CALL_ID_NUM);
        values.put(TARGET_MSISDN, TEST_VALUE_TARGET_MSISDN);
        values.put(TARGET_IMSI, TEST_VALUE_TARGET_IMSI);
        values.put(TARGET_IMEI, TEST_VALUE_TARGET_IMEI);
        values.put(LCS_CLIENT_ID, TEST_VALUE_LCS_CLIENT_ID);
        values.put(POSITION_DELIVERY, TEST_VALUE_POSITION_DELIVERY);
        values.put(RECORD_SEQ_NUM, TEST_VALUE_RECORD_SEQ_NUM);
        values.put(NETWORK_CALL_REFERENCE, TEST_VALUE_NETWORK_CALL_REFERENCE);
        values.put(MCC, TEST_VALUE_MCC);
        values.put(MNC, TEST_VALUE_MNC);
        values.put(RAC, TEST_VALUE_RAC);
        values.put(LAC, TEST_VALUE_LAC);
        values.put(DATETIME_ID, timestamp);
        //
        values.put(EXTERNAL_PROTOCOL_ID, TEST_VALUE_EXTERNAL_PROTOCOL_ID);
        insertRow(tableName, values);
    }

    /**
     * 
     * @param eventId
     * @param timestamp
     * @param tableName
     * @throws SQLException
     * @throws ParseException 
     */
    private void populateSMSRawTablesForQuery(final String eventId, final String timestamp, final String tableName)
            throws SQLException, ParseException {
        final Map<String, Object> values = new HashMap<String, Object>();
        final String dateId = getDateId(timestamp);
        final int hourId = getHourId(timestamp);
        values.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        values.put(HIER3_ID, TEST_VALUE_MSS_HIER3_ID);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        values.put(RAT, RAT_FOR_GSM);
        values.put(EVENT_ID, eventId);
        values.put(DATE_ID, dateId);
        values.put(HOUR_ID, hourId);
        values.put(SMS_RESULT, TEST_VALUE_SMS_RESULT);
        values.put(MSG_TYPE_INDICATOR, TEST_VALUE_MSG_TYPE_INDICATOR);
        values.put(EVENT_TIME, timestamp);
        values.put(MSISDN, TEST_VALUE_MSISDN);
        values.put(IMSI, TEST_VALUE_IMSI);
        values.put(TAC, SAMPLE_TAC);
        values.put(BEARER_SERVICE_CODE, TEST_VALUE_BEARER_SERVICE_CODE);
        values.put(TELE_SERVICE_CODE, TEST_VALUE_TELE_SERVICE_CODE);
        values.put(CALL_ID_NUM, TEST_VALUE_CALL_ID_NUM);
        values.put(TYPE_OF_CALLING_SUB, TEST_VALUE_TYPE_OF_CALLING_SUB);
        values.put(CALLING_PARTY_NUM, TEST_VALUE_CALLING_PARTY_NUM);
        values.put(CALLED_PARTY_NUM, TEST_VALUE_CALLED_PARTY_NUM);
        values.put(CALLING_SUB_IMSI, TEST_VALUE_CALLING_SUB_IMSI);
        values.put(CALLED_SUB_IMSI, TEST_VALUE_CALLED_SUB_IMSI);
        values.put(CALLING_SUB_IMEI, TEST_VALUE_CALLING_SUB_IMEI);
        values.put(CALLED_SUB_IMEI, TEST_VALUE_CALLED_SUB_IMEI);
        values.put(CALLING_SUB_IMEISV, TEST_VALUE_CALLING_SUB_IMEISV);
        values.put(CALLED_SUB_IMEISV, TEST_VALUE_CALLED_SUB_IMEISV);
        values.put(ORIGINATING_NUM, TEST_VALUE_ORIGINATING_NUM);
        values.put(DEST_NUM, TEST_VALUE_DEST_NUM);
        values.put(SERVICE_CENTRE, TEST_VALUE_SERVICE_CENTRE);
        values.put(ORIGINATING_TIME, TEST_VALUE_ORIGINATING_TIME);
        values.put(DELIVERY_TIME, TEST_VALUE_DELIVERY_TIME);
        values.put(RECORD_SEQ_NUM, TEST_VALUE_RECORD_SEQ_NUM);
        values.put(MCC, TEST_VALUE_MCC);
        values.put(MNC, TEST_VALUE_MNC);
        values.put(RAC, TEST_VALUE_RAC);
        values.put(LAC, TEST_VALUE_LAC);
        values.put(DATETIME_ID, timestamp);
        insertRow(tableName, values);
    }

    /**
     * 
     * @param eventId
     * @param eventType
     * @throws SQLException
     */
    private void populateVoiceTopologyTablesForQuery(final String eventId, final String eventType) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put("EVENT_ID_DESC", eventType);
        values.put(EVENT_ID, eventId);
        insertRow(TEMP_DIM_E_MSS_EVENTTYPE, values);

        values.clear();
        values.put(FAULT_CODE, TEST_VALUE_FAULT_CODE);
        values.put(FAULT_CODE_DESC, TEST_VALUE_FAULT_CODE_DESC);
        values.put(ADVICE, TEST_VALUE_ADVICE);
        insertRow(TEMP_DIM_E_MSS_FAULT_CODE, values);

        values.clear();
        values.put(INTERNAL_CAUSE_CODE, TEST_VALUE_INTERNAL_CAUSE_CODE);
        values.put(INTERNAL_CAUSE_CODE_DESC, TEST_VALUE_INTERNAL_CAUSE_CODE_DESC);
        insertRow(TEMP_DIM_E_MSS_INTERNAL_CAUSE_CODE, values);

        values.clear();
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, TEST_VALUE_MSS_CONTROLLER_NAME);
        values.put(HIERARCHY_1, TEST_VALUE_MSS_CELL_NAME);
        values.put(HIER3_ID, TEST_VALUE_MSS_HIER3_ID);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        insertRow(TEMP_DIM_E_SGEH_HIER321, values);

        values.clear();
        values.put(TAC, SAMPLE_TAC);
        values.put(MANUFACTURER, TEST_VALUE_MANUFACTURER);
        values.put(MARKETING_NAME, TEST_VALUE_MARKETING_NAME);
        insertRow(TEMP_DIM_E_SGEH_TAC, values);

        values.clear();
        values.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        values.put(EVENT_SOURCE_NAME, TEST_VALUE_MSS_EVNTSRC);
        insertRow(TEMP_DIM_E_MSS_EVNTSRC, values);

        values.clear();
        values.put(RAT, RAT_FOR_GSM);
        values.put(RAT_DESC, GSM);
        insertRow(TEMP_DIM_E_SGEH_RAT, values);
    }

    /**
     * 
     * @param eventId
     * @param eventType
     * @throws SQLException
     */
    private void populateLocTopologyTablesForQuery(final String eventId, final String eventType) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put("EVENT_ID_DESC", eventType);
        values.put(EVENT_ID, eventId);
        insertRow(TEMP_DIM_E_MSS_EVENTTYPE, values);

        values.clear();
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, TEST_VALUE_MSS_CONTROLLER_NAME);
        values.put(HIERARCHY_1, TEST_VALUE_MSS_CELL_NAME);
        values.put(HIER3_ID, TEST_VALUE_MSS_HIER3_ID);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        insertRow(TEMP_DIM_E_SGEH_HIER321, values);

        values.clear();
        values.put(TAC, SAMPLE_TAC);
        values.put(MANUFACTURER, TEST_VALUE_MANUFACTURER);
        values.put(MARKETING_NAME, TEST_VALUE_MARKETING_NAME);
        insertRow(TEMP_DIM_E_SGEH_TAC, values);

        values.clear();
        values.put(RAT, RAT_FOR_GSM);
        values.put(RAT_DESC, GSM);
        insertRow(TEMP_DIM_E_SGEH_RAT, values);

        values.clear();
        values.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        values.put(EVENT_SOURCE_NAME, TEST_VALUE_MSS_EVNTSRC);
        insertRow(TEMP_DIM_E_MSS_EVNTSRC, values);

        values.clear();
        values.put(LCS_CLIENT_TYPE, TEST_VALUE_LCS_CLIENT_TYPE);
        values.put(LCS_CLIENT_TYPE_DESC, TEST_VALUE_LCS_CLIENT_TYPE_DESC);
        insertRow(TEMP_DIM_E_MSS_LCS_CLIENT_TYPE, values);

        values.clear();
        values.put(TYPE_LOCATION_REQ, TEST_VALUE_TYPE_LOCATION_REQ);
        values.put(TYPE_LOCATION_REQ_DESC, TEST_VALUE_TYPE_LOCATION_REQ_DESC);
        insertRow(TEMP_DIM_E_MSS_TYPE_LOCATION_REQ, values);

        values.clear();
        values.put(UNSUC_POSITION_REASON, TEST_VALUE_UNSUC_POSITION_REASON);
        values.put(UNSUC_POSITION_REASON_DESC, TEST_VALUE_UNSUC_POSITION_REASON_DESC);
        insertRow(TEMP_DIM_E_MSS_UNSUC_POSITION_REASON, values);
    }

    /**
     * 
     * @param eventId
     * @param eventType
     * @throws SQLException
     */
    private void populateSMSTopologyTablesForQuery(final String eventId, final String eventType) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put("EVENT_ID_DESC", eventType);
        values.put(EVENT_ID, eventId);
        insertRow(TEMP_DIM_E_MSS_EVENTTYPE, values);

        values.clear();
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, TEST_VALUE_MSS_CONTROLLER_NAME);
        values.put(HIERARCHY_1, TEST_VALUE_MSS_CELL_NAME);
        values.put(HIER3_ID, TEST_VALUE_MSS_HIER3_ID);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        insertRow(TEMP_DIM_E_SGEH_HIER321, values);

        values.clear();
        values.put(TAC, SAMPLE_TAC);
        values.put(MANUFACTURER, TEST_VALUE_MANUFACTURER);
        values.put(MARKETING_NAME, TEST_VALUE_MARKETING_NAME);
        insertRow(TEMP_DIM_E_SGEH_TAC, values);

        values.clear();
        values.put(RAT, RAT_FOR_GSM);
        values.put(RAT_DESC, GSM);
        insertRow(TEMP_DIM_E_SGEH_RAT, values);

        values.clear();
        values.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        values.put(EVENT_SOURCE_NAME, TEST_VALUE_MSS_EVNTSRC);
        insertRow(TEMP_DIM_E_MSS_EVNTSRC, values);

        values.clear();
        values.put(MSG_TYPE_INDICATOR, TEST_VALUE_MSG_TYPE_INDICATOR);
        values.put(MSG_TYPE_INDICATOR_DESC, TEST_VALUE_MSG_TYPE_INDICATOR_DESC);
        insertRow(TEMP_DIM_E_MSS_MSG_TYPE_INDICATOR, values);

        values.clear();
        values.put(SMS_RESULT, TEST_VALUE_SMS_RESULT);
        values.put(SMS_RESULT_DESC, TEST_VALUE_SMS_RESULT_DESC);
        insertRow(TEMP_DIM_E_MSS_SMS_RESULT, values);
    }

    /**
     * 
     * @param eventId
     * @param eventType
     * @param timestamp
     * @throws SQLException
     */
    private void populateVoiceData(final String eventId, final String eventType, final String timestamp)
            throws SQLException {
        //populate data for raw tables
        populateVoiceRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW);
        populateVoiceRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW);
        populateVoiceRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW);

        populateVoiceTopologyTablesForQuery(eventId, eventType);
    }

    /**
     * @param eventId
     * @param eventType
     * @param timestamp
     * @throws ParseException 
     */
    private void populateLocData(final String eventId, final String eventType, final String timestamp)
            throws SQLException, ParseException {
        //populate data for raw tables
        populateLocRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_ERR_RAW);
        populateLocRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_SUC_RAW);

        populateLocTopologyTablesForQuery(eventId, eventType);
    }

    /**
     * @param eventId
     * @param eventType
     * @param timestamp
     * @throws ParseException 
     */
    private void populateSMSData(final String eventId, final String eventType, final String timestamp)
            throws SQLException, ParseException {
        //populate data for raw tables
        populateSMSRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_SMS_CDR_ERR_RAW);
        populateSMSRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_SMS_CDR_SUC_RAW);

        populateSMSTopologyTablesForQuery(eventId, eventType);
    }

    private String getDateId(final String timestamp) throws ParseException {
        final DateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final DateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd");
        final Date date = dateFormat1.parse(timestamp);

        final Calendar calendar = Calendar.getInstance(new SimpleTimeZone(0, "GMT"));
        dateFormat2.setCalendar(calendar);

        calendar.setTime(date);
        return dateFormat2.format(calendar.getTime());
    }

    @SuppressWarnings("deprecation")
    private int getHourId(final String timestamp) throws ParseException {
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final Date date = dateFormat.parse(timestamp);

        return date.getHours();
    }

    /**
     * 
     * @param eventID
     * @param timerange
     * @return
     * @throws Exception
     */
    private String getVoiceJsonString(final String eventID, final String timerange) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, timerange);
        map.putSingle(EVENT_ID_PARAM, eventID);
        map.putSingle(HIER3_ID, String.valueOf(TEST_VALUE_MSS_HIER3_ID));
        map.putSingle(EVNTSRC_ID, String.valueOf(TEST_VALUE_MSS_EVNTSRC_ID));
        map.putSingle(HIER321_ID, String.valueOf(TEST_VALUE_MSS_HIER321_ID));
        map.putSingle(FAULT_CODE_PARAM, String.valueOf(TEST_VALUE_FAULT_CODE));
        map.putSingle(INTERNAL_CAUSE_CODE_PARAM, String.valueOf(TEST_VALUE_INTERNAL_CAUSE_CODE));
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "50");

        DummyUriInfoImpl.setUriInfoMss(map, mssKPIRatioResource);

        return mssKPIRatioResource.getData();
    }

    /**
     * 
     * @param eventID
     * @param timerange
     * @return
     * @throws Exception
     */
    private String getLocOrSMSJsonString(final String eventID, final String timerange) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, timerange);
        map.putSingle(EVENT_ID_PARAM, eventID);
        map.putSingle(HIER3_ID, String.valueOf(TEST_VALUE_MSS_HIER3_ID));
        map.putSingle(EVNTSRC_ID, String.valueOf(TEST_VALUE_MSS_EVNTSRC_ID));
        map.putSingle(HIER321_ID, String.valueOf(TEST_VALUE_MSS_HIER321_ID));
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "50");

        DummyUriInfoImpl.setUriInfoMss(map, mssKPIRatioResource);

        return mssKPIRatioResource.getData();
    }

    /**
     * @param summaryResult
     * @param eventId
     * @param eventType
     * @param noOfErrors
     * @param noOfSuccess
     * @param total
     * @param successRatio
     * @param impactedSubScribers
     */
    private void validateVoiceResultFromTables(final List<MssEventAnalysisDetailedResult> results,
            final String eventType) {
        assertThat(results.size(), is(2));
        final MssEventAnalysisDetailedResult result1 = results.get(0);
        final MssEventAnalysisDetailedResult result2 = results.get(1);
        assertThat(result1.getInternalCauseCodeID(), is(String.valueOf(TEST_VALUE_INTERNAL_CAUSE_CODE)));
        assertThat(result2.getInternalCauseCodeID(), is(String.valueOf(TEST_VALUE_INTERNAL_CAUSE_CODE)));

        assertThat(result1.getEventIdDesc(), is(eventType));
        assertThat(result2.getEventIdDesc(), is(eventType));

        assertThat(result1.getFaultCodeID(), is(String.valueOf(TEST_VALUE_FAULT_CODE)));
        assertThat(result2.getFaultCodeID(), is(String.valueOf(TEST_VALUE_FAULT_CODE)));
    }

    /**
     * @param summaryResult
     * @param eventId
     * @param eventType
     * @param noOfErrors
     * @param noOfSuccess
     * @param total
     * @param successRatio
     * @param impactedSubScribers
     */
    private void validateLocResultFromTables(final List<MssEventAnalysisDetailedResult> results, final String eventType) {
        assertThat(results.size(), is(3));
        final MssEventAnalysisDetailedResult result = results.get(0);

        assertThat(result.getCell(), is(TEST_VALUE_MSS_CELL_NAME));
        assertThat(result.getController(), is(TEST_VALUE_MSS_CONTROLLER_NAME));
        assertThat(result.getEventSourceName(), is(TEST_VALUE_MSS_EVNTSRC));
        assertThat(result.getEventIdDesc(), is(eventType));
    }

    @Test
    public void testTypeMSCDrilltypeEvents_Voice_RAW() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(15);
        final String eventId = MSS_MS_ORIGINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_MS_ORIGINATING;

        createVoiceRawTables();
        createVoiceTopologyTables();

        populateVoiceData(eventId, eventType, timestamp);

        final String json = getVoiceJsonString(eventId, THIRTY_MINUTES);

        final List<MssEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                MssEventAnalysisDetailedResult.class);

        validateVoiceResultFromTables(summaryResult, eventType);
    }

    @Test
    public void testTypeMSCDrilltypeEvents_Loc_RAW() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(15);
        final String eventId = MSS_LOCATION_SERVICE_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_LOC;

        createLocRawTables();
        createLocTopologyTables();

        populateLocData(eventId, eventType, timestamp);

        final String json = getLocOrSMSJsonString(eventId, THIRTY_MINUTES);

        final List<MssEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                MssEventAnalysisDetailedResult.class);

        validateLocResultFromTables(summaryResult, eventType);
    }

    @Test
    public void testTypeMSCDrilltypeEvents_Sms_RAW() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(15);
        final String eventId = MSS_SMS_MS_ORIGINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_SMS_MS_ORIGINATING;

        createSMSRawTables();
        createSMSTopologyTables();

        populateSMSData(eventId, eventType, timestamp);

        final String json = getLocOrSMSJsonString(eventId, THIRTY_MINUTES);

        final List<MssEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                MssEventAnalysisDetailedResult.class);

        validateLocResultFromTables(summaryResult, eventType);
    }
}
