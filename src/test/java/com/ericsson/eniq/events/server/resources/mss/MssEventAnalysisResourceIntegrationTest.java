package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MssEventAnalysisResourceIntegrationTest extends MssDataServiceBaseTestCase {

    private static final String TIME_VALUE_GREATER_THAN_WEEK = "10100";

    private static final String TIME_VALUE_ONE_DAY = "1440";

    private MultivaluedMap<String, String> map;

    private MssEventAnalysisResource mssEvntAnalysis;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String TIME = "30";

    private static final String TIME_FROM = "1500";

    private static final String TIME_TO = "1600";

    private static final String DATE_FROM = "11052010";

    private static final String DATE_TO = "12052010";

    private static final String NODE = "ONRM_RootMo_R:RNC01:RNC01,ERICSSON,1";

    private static final String TIME_VALUE_FIVE_MINUTES = "5";

    private static final String MAX_ROWS_VALUE = "50";
    
    private static final String FTYPE_VALUE_BLOCKED = "BLOCKED";
	
	private static final String FTYPE_VALUE_DROPPED = "DROPPED";


    @Override
    public void onSetUp() {
        mssEvntAnalysis = new MssEventAnalysisResource();
        mssEvntAnalysis.queryConstructor = this.queryConstructor;
        attachDependenciesForMssBaseResource(mssEvntAnalysis);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetSummaryDataByTime_Manufacturer_5Minutes() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle("manufacturer", "Ningbo%20Bird%20Co%20Ltd%20%2899%20Chenhshan%20/999%20Dachen%29");
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_Manufacturer_1Day() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle("manufacturer", "Ningbo%20Bird%20Co%20Ltd%20%2899%20Chenhshan%20/999%20Dachen%29");
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_Manufacturer_1Week() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle("manufacturer", "Ningbo%20Bird%20Co%20Ltd%20%2899%20Chenhshan%20/999%20Dachen%29");
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_TAC_5_minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_TAC_1_day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_TAC_1_week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSC_5_minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(NODE_PARAM, "MSS_1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSC_1_day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(NODE_PARAM, "MSC1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSC_1_week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(NODE_PARAM, "MSC1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSC_5_Minutes() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSC_30_Minutes() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSC_1_Day() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSC_1_Week() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_CELL() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(NODE_PARAM, "CELL3000,,BSC1,ERICSSON,1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_Imsi() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, "123456789098765");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_Imsi_1_Day() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, "123456789098765");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_Imsi_1_Week() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, "123456789098765");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSISDN() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "123456789098765");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSISDN_1_Day() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "123456789098765");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSISDN_1_Week() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "123456789098765");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_TACGroup_5_minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_TACGroup_1_day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_TACGroup_1_week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_ExclusiveTACGroup_5_Min() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_ExclusiveTACGroup_1_day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_ExclusiveTACGroup_1_week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSCGroup_5_minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(GROUP_NAME_PARAM, "MSS_Group1");
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSCGroup_1_day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSCGroup_1_week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSCGroup_5_Minutes() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSCGroup_30_Minutes() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSCGroup_1_Day() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSCGroup_1_Week() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_CELLGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_TACGroupDrillDownVoice_5_minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_TACGroupDrillDownVoice_1_day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_TACGroupDrillDownVoice_1_week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_ExclusiveTACGroupDrillDownVoice_5_minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_ExclusiveTACGroupDrillDownVoice_1_day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_ExclusiveTACGroupDrillDownVoice_1_week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSCGroupDrillDownVoice_5_minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "MSS_Group1");
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSCGroupDrillDownVoice_1_day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSCGroupDrillDownVoice_1_week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSCGroupDrillDownVoice_5_Minutes() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSCGroupDrillDownVoice_30_Minutes() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSCGroupDrillDownVoice_1_Day() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSCGroupDrillDownVoice_1_Week() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_CELLGroupDrillDownVoice() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_TACGroupDrillDownLocationService_5_minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_TACGroupDrillDownLocationService_1_day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_TACGroupDrillDownLocationService_1_week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_ExclusiveTACGroupDrillDownLocationService_5_minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_ExclusiveTACGroupDrillDownLocationService_1_day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_ExclusiveTACGroupDrillDownLocationService_1_week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSCGroupDrillDownLocationService_5_minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "MSS_Group1");
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSCGroupDrillDownLocationService_1_day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_MSCGroupDrillDownLocationService_1_week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSCGroupDrillDownLocationService_5_Minutes() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSCGroupDrillDownLocationService_30_Minutes() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSCGroupDrillDownLocationService_1_Day() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_BSCGroupDrillDownLocationService_1_Week() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_CELLGroupDrillDownLocationService() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_ImsiGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_ImsiGroup_1_Day() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByTime_ImsiGroup_1_Week() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_WEEK);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_CELLGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_BSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_MSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_TACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_ExclusiveTACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_CELL() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(CELL_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_BSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(CONTROLLER_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_MSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_TAC() throws Exception {
        map.clear();

        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_MAN() throws Exception {
        map.clear();

        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle("manufacturer", "Sagem (75783 Paris)");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_ICC_FC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(INTERNAL_CAUSE_CODE_PARAM, "6");
        map.putSingle(FAULT_CODE_PARAM, "3758");
        map.putSingle(EVENT_SOURCE_SQL_ID, "8347002656519852921");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_CELLGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_BSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_MSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_TACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_ExclusiveTACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_CELL() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(CELL_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_BSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(CONTROLLER_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_MSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_TAC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_MAN() throws Exception {
        map.clear();

        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle("manufacturer", "Sagem (75783 Paris)");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_CELLGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_BSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_MSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_TACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_ExclusiveTACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_CELL() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(CELL_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_BSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(CONTROLLER_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_MSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_TAC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_MAN() throws Exception {
        map.clear();

        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle("manufacturer", "Sagem (75783 Paris)");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_imsi() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(IMSI_PARAM, "123456789098765");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_msisdn() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(MSISDN_PARAM, "123456789098765");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_CallForwardEvent_MSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "2");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_RoamingEvent_MSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "3");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_CallForwardEvent_MSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "2");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_RoamingEvent_MSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "3");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_CallForwardEvent_TAC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "2");
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_RoamingEvent_TAC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "3");
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_CallForwardEvent_TACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "2");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_RoamingEvent_TACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "3");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_LocService_CELLGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_LocService_BSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_LocService_MSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_LocService_TACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_LocService_ExclusiveTACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_LocService_CELL() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(CELL_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_LocService_BSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(CONTROLLER_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_LocService_MSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_LocService_TAC() throws Exception {
        map.clear();

        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_LocService_MAN() throws Exception {
        map.clear();

        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle("manufacturer", "Sagem (75783 Paris)");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_LocService_CELLGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_LocService_BSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_LocService_MSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_LocService_TACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_LocService_ExclusiveTACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_LocService_CELL() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(CELL_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_LocService_BSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(CONTROLLER_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_LocService_MSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_LocService_TAC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_LocService_MAN() throws Exception {
        map.clear();

        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle("manufacturer", "Sagem (75783 Paris)");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_LocService_CELLGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_LocService_BSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_LocService_MSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_LocService_TACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_LocService_ExclusiveTACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_LocService_CELL() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(CELL_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_LocService_BSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(CONTROLLER_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_LocService_MSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_LocService_TAC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_LocService_MAN() throws Exception {
        map.clear();

        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle("manufacturer", "Sagem (75783 Paris)");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_LocService_imsi() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(IMSI_PARAM, "123456789098765");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_LocService_msisdn() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(MSISDN_PARAM, "123456789098765");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    /////////////////////////////

    @Test
    public void testGetErrReportDataByTime_SMS_CELLGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_SMS_BSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_SMS_MSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_SMS_TACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_SMS_ExclusiveTACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_SMS_CELL() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(CELL_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_SMS_BSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(CONTROLLER_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_SMS_MSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_SMS_TAC() throws Exception {
        map.clear();

        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetErrReportDataByTime_SMS_MAN() throws Exception {
        map.clear();

        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle("manufacturer", "Sagem (75783 Paris)");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_SMS_CELLGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_SMS_BSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_SMS_MSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_SMS_TACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_SMS_ExclusiveTACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_SMS_CELL() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(CELL_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_SMS_BSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(CONTROLLER_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_SMS_MSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_SMS_TAC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSucReportDataByTime_SMS_MAN() throws Exception {
        map.clear();

        map.putSingle(KEY_PARAM, KEY_TYPE_SUC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle("manufacturer", "Sagem (75783 Paris)");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_SMS_CELLGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_SMS_BSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_SMS_MSCGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_SMS_TACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_SMS_ExclusiveTACGroup() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_SMS_CELL() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(CELL_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_SMS_BSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(CONTROLLER_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_SMS_MSC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, "53739127664802763");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_SMS_TAC() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_SMS_MAN() throws Exception {
        map.clear();

        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle("manufacturer", "Sagem (75783 Paris)");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_SMS_imsi() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(IMSI_PARAM, "123456789098765");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTotalReportDataByTime_SMS_msisdn() throws Exception {
        map.clear();
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(MSISDN_PARAM, "123456789098765");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testVerifyDisplayType() throws Exception {
        final String invalidDisplayType = "error";

        map.clear();
        map.putSingle(DISPLAY_PARAM, invalidDisplayType);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONErrorResult(result);
        assertResultContains(result, E_NO_SUCH_DISPLAY_TYPE);
    }

    @Test
    public void testMissingDisplayParam() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }

    @Test
    public void testMissingTypeParam() throws Exception {
        map.clear();
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }

    @Test
    public void testMissingDisplayAndTypeParam() throws Exception {
        map.clear();
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }

    @Test
    public void testCheckValidBSC() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(NODE_PARAM, "dfasdaf");
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONErrorResult(result);
    }
    
	@Test
    public void testMSCBlockedCallFlow() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, "360");
        map.putSingle(NODE_PARAM, "MSS_5");
        map.putSingle(TZ_OFFSET, "+0530");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(FAIL_TYPE, FTYPE_VALUE_BLOCKED);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }
	
	@Test
    public void testMSCDroppedCallFlow() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, "360");
        map.putSingle(NODE_PARAM, "MSS_5");
        map.putSingle(TZ_OFFSET, "+0530");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(FAIL_TYPE, FTYPE_VALUE_DROPPED);
        final String result = mssEvntAnalysis.getData("requestID", map);
        assertJSONSucceeds(result);
    }

}
