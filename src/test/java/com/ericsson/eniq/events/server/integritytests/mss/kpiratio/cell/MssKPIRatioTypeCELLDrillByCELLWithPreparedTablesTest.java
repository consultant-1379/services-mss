/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.mss.kpiratio.cell;

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
import com.ericsson.eniq.events.server.test.queryresults.mss.MssKPIRatioDrillTypeCELLResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * 
 * @author echimma
 * @since 2011
 *
 */
public class MssKPIRatioTypeCELLDrillByCELLWithPreparedTablesTest extends
        MssTestsWithTemporaryTablesBaseTestCase<MssKPIRatioDrillTypeCELLResult> {
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
    private final Collection<String> getRawColumns() {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.clear();
        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(HIER321_ID);
        columnsForRawTable.add(INTERNAL_CAUSE_CODE);
        columnsForRawTable.add(FAULT_CODE);
        columnsForRawTable.add(RAT);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(DATETIME_ID);
        return columnsForRawTable;
    }

    /**
     * 
     * @throws Exception
     */
    private void createVoiceRawTables() throws Exception {
        final Collection<String> columnsForRawTable = getRawColumns();

        createTemporaryTable(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, columnsForRawTable);
        createTemporaryTable(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, columnsForRawTable);
        createTemporaryTable(TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW, columnsForRawTable);
    }

    /**
     * 
     * @throws Exception
     */
    private void createTopologyTables() throws Exception {
        final Collection<String> columnsForTopologyTable = new ArrayList<String>();
        columnsForTopologyTable.add(EVENT_ID);
        columnsForTopologyTable.add("EVENT_ID_DESC");
        createTemporaryTable(TEMP_DIM_E_MSS_EVENTTYPE, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(FAULT_CODE);
        columnsForTopologyTable.add(FAULT_CODE_DESC);
        createTemporaryTable(TEMP_DIM_E_MSS_FAULT_CODE, columnsForTopologyTable);

        columnsForTopologyTable.clear();
        columnsForTopologyTable.add(INTERNAL_CAUSE_CODE);
        columnsForTopologyTable.add(INTERNAL_CAUSE_CODE_DESC);
        createTemporaryTable(TEMP_DIM_E_MSS_INTERNAL_CAUSE_CODE, columnsForTopologyTable);
    }

    /**
     * 
     * @param eventId
     * @param timestamp
     * @param tableName
     * @throws SQLException
     */
    private void populateRawTablesForQuery(final String eventId, final String timestamp, final String tableName)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        values.put(INTERNAL_CAUSE_CODE, TEST_VALUE_INTERNAL_CAUSE_CODE);
        values.put(FAULT_CODE, TEST_VALUE_FAULT_CODE);
        values.put(RAT, RAT_FOR_GSM);
        values.put(TAC, SAMPLE_TAC);
        values.put(IMSI, TEST_VALUE_IMSI);
        values.put(DATETIME_ID, timestamp);

        insertRow(tableName, values);
    }

    /**
     * 
     * @param eventId
     * @param eventType
     * @throws SQLException
     */
    private void populateTopologyTablesForQuery(final String eventId, final String eventType) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put("EVENT_ID_DESC", eventType);
        values.put(EVENT_ID, eventId);
        insertRow(TEMP_DIM_E_MSS_EVENTTYPE, values);

        values.clear();
        values.put(FAULT_CODE, TEST_VALUE_FAULT_CODE);
        values.put(FAULT_CODE_DESC, TEST_VALUE_FAULT_CODE_DESC);
        insertRow(TEMP_DIM_E_MSS_FAULT_CODE, values);

        values.clear();
        values.put(INTERNAL_CAUSE_CODE, TEST_VALUE_INTERNAL_CAUSE_CODE);
        values.put(INTERNAL_CAUSE_CODE_DESC, TEST_VALUE_INTERNAL_CAUSE_CODE_DESC);
        insertRow(TEMP_DIM_E_MSS_INTERNAL_CAUSE_CODE, values);
    }

    /**
     * 
     * @param eventId
     * @param eventType
     * @param timestamp
     * @throws SQLException
     */
    private void populateData(final String eventId, final String eventType, final String timestamp) throws SQLException {
        //populate data for raw tables
        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW);
        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW);
        populateRawTablesForQuery(eventId, timestamp, TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW);

        populateTopologyTablesForQuery(eventId, eventType);
    }

    /**
     * 
     * @param eventID
     * @param timerange
     * @return
     * @throws Exception
     */
    private String getJsonString(final String eventID, final String timerange) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, timerange);
        map.putSingle(EVENT_ID_PARAM, eventID);
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
    private void validateResultFromTables(final List<MssKPIRatioDrillTypeCELLResult> results, final String eventId,
            final String eventType, final int occurrences, final int impactedSubScribers) {
        assertThat(results.size(), is(1));
        final MssKPIRatioDrillTypeCELLResult result = results.get(0);

        assertThat(result.getEventId(), is(eventId));
        assertThat(result.getHier321Id(), is(String.valueOf(TEST_VALUE_MSS_HIER321_ID)));
        assertThat(result.getInternalCauseCodeId(), is(TEST_VALUE_INTERNAL_CAUSE_CODE));
        assertThat(result.getInternalCauseCode(), is(TEST_VALUE_INTERNAL_CAUSE_CODE_DESC));
        assertThat(result.getFaultCodeId(), is(TEST_VALUE_FAULT_CODE));
        assertThat(result.getFaultCode(), is(TEST_VALUE_FAULT_CODE_DESC));
        assertThat(result.getEventType(), is(eventType));
        assertThat(result.getOccurrences(), is(occurrences));
        assertThat(result.getImpactedSubscribers(), is(impactedSubScribers));
    }

    @Test
    public void testTypeCELLDrilltypeCELL_RAW() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(15);
        final String eventId = MSS_MS_ORIGINATING_EVENT_ID;
        final String eventType = TEST_VALUE_EVENT_TYPE_MS_ORIGINATING;
        final int occurrences = 2;
        final int impactedSubScribers = 1;

        createVoiceRawTables();
        createTopologyTables();

        populateData(eventId, eventType, timestamp);

        final String json = getJsonString(eventId, THIRTY_MINUTES);

        final List<MssKPIRatioDrillTypeCELLResult> summaryResult = getTranslator().translateResult(json,
                MssKPIRatioDrillTypeCELLResult.class);

        validateResultFromTables(summaryResult, eventId, eventType, occurrences, impactedSubScribers);
    }
}
