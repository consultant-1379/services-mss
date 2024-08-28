/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.mss.internalcausecode.summary;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.integritytests.mss.MssTestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.resources.mss.MssCauseCodeAnalysisResource;
import com.ericsson.eniq.events.server.test.queryresults.mss.InternalCauseCodeAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.sql.SQLCommand;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author egraman
 * @since 2011
 * InternalCauseCodeAnalysisResourceWithPreparedRawTablesSummaryTest
 */
public class ICCAnalysisResourceWithPreparedRawTablesSumTest extends
        MssTestsWithTemporaryTablesBaseTestCase<InternalCauseCodeAnalysisSummaryResult> {

    private final static int internalCauseCode_7 = 7;

    private final static int internalCauseCode_17 = 17;

    private final static int internalCauseCode_29 = 29;

    private final static int internalCauseCode_38 = 38;

    private final static String faultCode_2 = "2";

    private final static String faultCode_15 = "15";

    private final static String faultCode_27 = "27";

    private final static String faultCode_38 = "38";

    private static final int IMSI_1 = 123456;

    private static final int IMSI_2 = 123455;

    private static final int TAC_1 = 1234567;

    private final MssCauseCodeAnalysisResource mssCauseCodeAnalysisResource = new MssCauseCodeAnalysisResource();

    private final static List<String> tempTables = new ArrayList<String>();

    private final static Map<Integer, String> internalCauseCodeMapping = new HashMap<Integer, String>();

    private final static MultivaluedMap<String, String> faultCodeMapping = new MultivaluedMapImpl();

    private final static MultivaluedMap<String, String> timeRangeMapping = new MultivaluedMapImpl();

    static {
        // Populate MSS internal cause codes/descriptions
        internalCauseCodeMapping.put(internalCauseCode_7, "Number changed");
        internalCauseCodeMapping.put(internalCauseCode_17, "Switching equipment congestion");
        internalCauseCodeMapping.put(internalCauseCode_29, "Identified channel does not exist");
        internalCauseCodeMapping.put(internalCauseCode_38,
                "Message not compatible with call state or non-existent or not implemented");
        // Populate MSS fault codes/descriptions
        faultCodeMapping
                .put(faultCode_2,
                        new ArrayList<String>(
                                Arrays.asList("1. COS = Traffic case not permitted 2. Redirection F, M.",
                                        "Called subscriber is temporarily barred for all incoming traffic. B-subscriber category TBI = 1.")));
        faultCodeMapping.put(
                faultCode_15,
                new ArrayList<String>(Arrays.asList("1. Redirection F, M. 2. ESS = Intercepted subscriber",
                        "Intercepted subscriber. B-subscriber category ICS =13.")));
        faultCodeMapping.put(
                faultCode_27,
                new ArrayList<String>(Arrays.asList(
                        "Redirection or normal end of selection signal ESS = faulty subscriber line.",
                        "Called subscribers line is out of order")));
        faultCodeMapping.put(
                faultCode_38,
                new ArrayList<String>(Arrays.asList("Send congestion signal COS = unpermitted traffic case(=14).",
                        "End-to-end signalling not allowed. Incorrect ANI protocol used.")));

        tempTables.add(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW);
        tempTables.add(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW);
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        attachDependenciesForMSSBaseResource(mssCauseCodeAnalysisResource);
        for (final String tempTable : tempTables) {
            createTemporaryTable(tempTable);
        }
        populateTemporaryTables();
        createTemporaryTables();
    }

    @Test
    public void testGetMssRankingData_InternalCauseCode_5Minutes_with_TAC_exclusion() throws Exception {

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(INTERNAL_CAUSE_CODE_PARAM, Integer.toString(internalCauseCode_7));
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisResource);

        final String json = mssCauseCodeAnalysisResource.getData();

        System.out.println(json);
        final List<InternalCauseCodeAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                InternalCauseCodeAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(0));
    }

    private void populateTemporaryTables() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -4);

            //error raw
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_7, faultCode_2, IMSI_1, TAC_1, sqlExecutor,
                    dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_7, faultCode_15, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_7, faultCode_27, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_7, faultCode_38, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_17, faultCode_2, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_17, faultCode_15, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_17, faultCode_27, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_17, faultCode_38, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_29, faultCode_2, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_29, faultCode_15, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_29, faultCode_27, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_29, faultCode_38, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_38, faultCode_2, IMSI_2, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_38, faultCode_15, IMSI_2, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_38, faultCode_27, IMSI_2, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, internalCauseCode_38, faultCode_38, IMSI_2, TAC_1,
                    sqlExecutor, dateTime);

            //drop call raw
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_7, faultCode_2, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_7, faultCode_15, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_7, faultCode_27, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_7, faultCode_38, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_17, faultCode_2, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_17, faultCode_15, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_17, faultCode_27, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_17, faultCode_38, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_29, faultCode_2, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_29, faultCode_15, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_29, faultCode_27, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_29, faultCode_38, IMSI_1, TAC_1,
                    sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_38, faultCode_2, IMSI_2, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_38, faultCode_15, IMSI_2, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_38, faultCode_27, IMSI_2, TAC_1,
                    sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, internalCauseCode_38, faultCode_38, IMSI_2, TAC_1,
                    sqlExecutor, dateTime);

            //TAC Exlusion
            insertRowIntoGroupTypeTac(TEMP_GROUP_TYPE_E_TAC, TAC_1, "EXCLUSIVE_TAC");

        } finally {
            closeSQLExector(sqlExecutor);
        }

    }

    private void insertRow(final String table, final int internalCauseCode, final String faultCode, final int testImsi,
            final int testTac, final SQLExecutor sqlExecutor, final String dateTime) throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values(" + internalCauseCode + "," + faultCode + ","
                + testImsi + "," + testTac + ",'" + dateTime + "')");
    }

    private void insertRowIntoGroupTypeTac(final String table, final int tac, final String grpName) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put("TAC", tac);
        valuesForTable.put("GROUP_NAME", grpName);
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }

    private void insertRowIntoICCTable(final String table, final int causeCode, final String iccDesc)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(INTERNAL_CAUSE_CODE_COLUMN, causeCode);
        valuesForTable.put(INTERNAL_CAUSE_CODE_DESC_COLUMN, iccDesc);
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }

    private void insertRowIntoFCTable(final String table, final String faultCode, final String fcDesc,
            final String fcAdvice) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(FAULT_CODE_COLUMN, faultCode);
        valuesForTable.put(FAULT_CODE_DESC_COLUMN, fcDesc);
        valuesForTable.put(FAULT_CODE_ADVICE_COLUMN, fcAdvice);
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }

    private void createTemporaryTable(final String tempTableName) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor.executeUpdate("create local temporary table " + tempTableName
                    + "(INTERNAL_CAUSE_CODE smallint, FAULT_CODE smallint, IMSI int, TAC int, "
                    + "DATETIME_ID timestamp)");

        } finally {
            closeSQLExector(sqlExecutor);
        }
    }

    private void populateICCTables() throws SQLException {
        for (final int internalCauseCode : internalCauseCodeMapping.keySet()) {
            insertRowIntoICCTable(TEMP_DIM_E_MSS_INTERNAL_CAUSE_CODE, internalCauseCode,
                    internalCauseCodeMapping.get(internalCauseCode));
        }
    }

    private void populateFCTables() throws SQLException {
        for (final String faultCode : faultCodeMapping.keySet()) {
            final List<String> values = faultCodeMapping.get(faultCode);
            insertRowIntoFCTable(TEMP_DIM_E_MSS_FAULT_CODE, faultCode, values.get(0), values.get(1));
        }
    }

    private void createTemporaryICCTables() throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(INTERNAL_CAUSE_CODE_COLUMN);
        columnsForTable.add(INTERNAL_CAUSE_CODE_DESC_COLUMN);
        new SQLCommand(connection).createTemporaryTable(TEMP_DIM_E_MSS_INTERNAL_CAUSE_CODE, columnsForTable);
    }

    private void createTemporaryFCTables() throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(FAULT_CODE_COLUMN);
        columnsForTable.add(FAULT_CODE_DESC_COLUMN);
        columnsForTable.add(FAULT_CODE_ADVICE_COLUMN);
        new SQLCommand(connection).createTemporaryTable(TEMP_DIM_E_MSS_FAULT_CODE, columnsForTable);
    }

    private void createTemporaryTables() throws Exception {
        createTemporaryICCTables();
        populateICCTables();

        createTemporaryFCTables();
        populateFCTables();
    }

}
