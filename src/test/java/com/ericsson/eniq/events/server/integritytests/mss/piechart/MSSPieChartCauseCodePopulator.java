/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.mss.piechart;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ericsson.eniq.events.server.test.sql.SQLCommand;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;

/**
 * @author eavidat
 * @since 2011
 *
 */
public class MSSPieChartCauseCodePopulator {
    public final Connection connection;

    public final static int internal_causeCode_1 = 1;

    public final static int internal_causeCode_2 = 2;

    public final static int internal_causeCode_3 = 3;

    public final static String internal_causeCode_1_desc = "ICC 1";

    public final static String internal_causeCode_2_desc = "ICC 2";

    public final static String internal_causeCode_3_desc = "ICC 3";

    public final static int faultCode_1 = 1;

    public final static int faultCode_2 = 2;

    public final static String faultCode_1_desc = "FC 1";

    public final static String faultCode_2_desc = "FC 2";

    public final static String faultCodeAdvice = "ADVICE !!!";

    public static final int TEST_IMSI_1 = 123456;

    public static final int TEST_IMSI_2 = 123455;

    public static final int TEST_IMSI_3 = 123457;

    public static final int TEST_TAC_1 = 1234567;

    public static final int TEST_TAC_2 = 1234569;

    private final String TEST_MSC_1_HASHID = "5779923676061823640";

    private final String TEST_BSC_1_HASHID = "5616525403763891229";

    private final String TEST_CELL_1_HASHID = "2828455554984998510";

    /**
     * @param connection
     */
    public MSSPieChartCauseCodePopulator(final Connection connection) {
        this.connection = connection;
    }

    public void createTemporaryTables() throws Exception {
        createTemporaryCCTables();
        createTemporaryFCTables();
        createTemporaryRawTables();
    }

    public void populateTemporaryTables() throws SQLException {
        populateCCTables();
        populateFCTables();
        populateRawTables();
    }

    private void createTemporaryRawTables() throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(INTERNAL_CAUSE_CODE);
        columnsForTable.add(FAULT_CODE);
        columnsForTable.add(DATETIME_ID);
        columnsForTable.add(IMSI);
        columnsForTable.add(TAC);
        columnsForTable.add(EVNTSRC_ID);
        columnsForTable.add(HIER3_ID);
        columnsForTable.add(HIER321_ID);
        new SQLCommand(connection).createTemporaryTableMss(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, columnsForTable);
        new SQLCommand(connection).createTemporaryTableMss(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, columnsForTable);
    }

    private void populateRawTables() throws SQLException {
        final String timeStamp = DateTimeUtilities.getDateTimeMinus25Minutes();

        insertRowIntoRawTable(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, TEST_IMSI_1, TEST_TAC_1, internal_causeCode_1,
                faultCode_1, timeStamp, TEST_MSC_1_HASHID, TEST_BSC_1_HASHID, TEST_CELL_1_HASHID);
        insertRowIntoRawTable(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, TEST_IMSI_2, TEST_TAC_2, internal_causeCode_1,
                faultCode_1, timeStamp, TEST_MSC_1_HASHID, TEST_BSC_1_HASHID, TEST_CELL_1_HASHID);
        insertRowIntoRawTable(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, TEST_IMSI_2, TEST_TAC_2, internal_causeCode_2,
                faultCode_2, timeStamp, TEST_MSC_1_HASHID, TEST_BSC_1_HASHID, TEST_CELL_1_HASHID);

        insertRowIntoRawTable(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, TEST_IMSI_1, TEST_TAC_1, internal_causeCode_1,
                faultCode_1, timeStamp, TEST_MSC_1_HASHID, TEST_BSC_1_HASHID, TEST_CELL_1_HASHID);
        insertRowIntoRawTable(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, TEST_IMSI_3, TEST_TAC_1, internal_causeCode_1,
                faultCode_1, timeStamp, TEST_MSC_1_HASHID, TEST_BSC_1_HASHID, TEST_CELL_1_HASHID);
        insertRowIntoRawTable(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, TEST_IMSI_2, TEST_TAC_1, internal_causeCode_2,
                faultCode_2, timeStamp, TEST_MSC_1_HASHID, TEST_BSC_1_HASHID, TEST_CELL_1_HASHID);
    }

    private void insertRowIntoRawTable(final String table, final int imsi, final int tac, final int causeCode,
            final int faultCode, final String timeStamp, final String eventSrcId, final String hier3Id,
            final String hier321Id) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(INTERNAL_CAUSE_CODE, causeCode);
        valuesForTable.put(FAULT_CODE, faultCode);
        valuesForTable.put(DATETIME_ID, timeStamp);
        valuesForTable.put(IMSI, imsi);
        valuesForTable.put(TAC, tac);
        valuesForTable.put(EVNTSRC_ID, Long.parseLong(eventSrcId));
        valuesForTable.put(HIER3_ID, Long.parseLong(hier3Id));
        valuesForTable.put(HIER321_ID, Long.parseLong(hier321Id));
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }

    private void createTemporaryFCTables() throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(FAULT_CODE);
        columnsForTable.add(FAULT_CODE_DESC);
        columnsForTable.add(FAULT_CODE_ADVICE_COLUMN);
        new SQLCommand(connection).createTemporaryTableMss(TEMP_DIM_E_MSS_FAULT_CODE, columnsForTable);
    }

    private void populateFCTables() throws SQLException {
        insertRowIntoFCTable(TEMP_DIM_E_MSS_FAULT_CODE, faultCode_1, faultCode_1_desc, faultCodeAdvice);
        insertRowIntoFCTable(TEMP_DIM_E_MSS_FAULT_CODE, faultCode_2, faultCode_2_desc, faultCodeAdvice);
    }

    private void insertRowIntoFCTable(final String table, final int faultCode, final String fcDesc,
            final String faultCodeAdvice1) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(FAULT_CODE, faultCode);
        valuesForTable.put(FAULT_CODE_DESC, fcDesc);
        valuesForTable.put(FAULT_CODE_ADVICE_COLUMN, faultCodeAdvice1);
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }

    private void createTemporaryCCTables() throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(INTERNAL_CAUSE_CODE);
        columnsForTable.add(INTERNAL_CAUSE_CODE_DESC);
        new SQLCommand(connection).createTemporaryTableMss(TEMP_DIM_E_MSS_INTERNAL_CAUSE_CODE, columnsForTable);
    }

    private void populateCCTables() throws SQLException {
        insertRowIntoCCTable(TEMP_DIM_E_MSS_INTERNAL_CAUSE_CODE, internal_causeCode_1, internal_causeCode_1_desc);
        insertRowIntoCCTable(TEMP_DIM_E_MSS_INTERNAL_CAUSE_CODE, internal_causeCode_2, internal_causeCode_2_desc);
        insertRowIntoCCTable(TEMP_DIM_E_MSS_INTERNAL_CAUSE_CODE, internal_causeCode_3, internal_causeCode_3_desc);
    }

    private void insertRowIntoCCTable(final String table, final int causeCode, final String ccDesc) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(INTERNAL_CAUSE_CODE, causeCode);
        valuesForTable.put(INTERNAL_CAUSE_CODE_DESC, ccDesc);
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }
}