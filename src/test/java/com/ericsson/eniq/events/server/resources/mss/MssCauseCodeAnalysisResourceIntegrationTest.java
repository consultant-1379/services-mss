/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author egraman
 * @since 2011
 * 
 */
public class MssCauseCodeAnalysisResourceIntegrationTest extends MssDataServiceBaseTestCase {

    private MultivaluedMap<String, String> map;

    private MssCauseCodeAnalysisResource mssCauseCodeAnalysisResource;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String COUNT = "30";

    private static final String THREE_HOURS = "180";

    private static final String TWO_WEEKS = "20160";

    private static final String MAX_ROWS_VALUE = "500";

    @Override
    public void onSetUp() {
        mssCauseCodeAnalysisResource = new MssCauseCodeAnalysisResource();

        attachDependenciesForMssBaseResource(mssCauseCodeAnalysisResource);

        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetCauseCodeAnalysisDataByInternalCauseCodeParam() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(INTERNAL_CAUSE_CODE_PARAM, "6");
        map.putSingle(TIME_QUERY_PARAM, COUNT);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataByInternalCauseCodeParam15MinAgg() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(INTERNAL_CAUSE_CODE_PARAM, "6");
        map.putSingle(TIME_QUERY_PARAM, THREE_HOURS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataByInternalCauseCodeParamDayAgg() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(INTERNAL_CAUSE_CODE_PARAM, "6");
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataMscParam() throws Exception {
        // MSS/NETWORK/INTERNAL_CAUSE_CODE_ANALYSIS?time=30&type=MSC&node=MSC10&display=grid&tzOffset=+0100&maxRows=500
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, MSC_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, "MSS_1");
        map.putSingle(TIME_QUERY_PARAM, COUNT);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataMscParam15MinAgg() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(NODE_PARAM, "MSS_1");
        map.putSingle(TIME_QUERY_PARAM, THREE_HOURS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataMscParamDayAgg() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, MSC_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, "MSS_1");
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataBscParam() throws Exception {
        // MSS/NETWORK/INTERNAL_CAUSE_CODE_ANALYSIS?time=30&type=BSC&node=BSC1,Ericsson,GSM&display=grid&tzOffset=+0100&maxRows=500
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, BSC_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, "BSC10,Ericsson,GSM");
        map.putSingle(TIME_QUERY_PARAM, COUNT);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataBscParam15MinAgg() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, BSC_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, "BSC10,Ericsson,GSM");
        map.putSingle(TIME_QUERY_PARAM, THREE_HOURS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataBscParamDayAgg() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, BSC_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, "BSC10,Ericsson,GSM");
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataCellParam() throws Exception {
        // MSS/NETWORK/INTERNAL_CAUSE_CODE_ANALYSIS?time=30&type=CELL&node=1,,ONRM_RootMo_R:RNC01:RNC01,Ericsson,3G&display=grid&tzOffset=+0100&maxRows=500
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, CELL_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, "1,,ONRM_RootMo_R:RNC01:RNC01,Ericsson,3G");
        map.putSingle(TIME_QUERY_PARAM, COUNT);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataCellParam15MinAgg() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, CELL_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, "1,,ONRM_RootMo_R:RNC01:RNC01,Ericsson,3G");
        map.putSingle(TIME_QUERY_PARAM, THREE_HOURS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataCellParamDayAgg() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, CELL_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, "1,,ONRM_RootMo_R:RNC01:RNC01,Ericsson,3G");
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataMscGroupParam() throws Exception {
        // MSS/NETWORK/INTERNAL_CAUSE_CODE_ANALYSIS?time=30&type=MSC&groupname=MSC_Group1&display=grid&tzOffset=+0100&maxRows=500
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, MSC_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, "MSC_Group1");
        map.putSingle(TIME_QUERY_PARAM, COUNT);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataMscGroupParam15MinAgg() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, MSC_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, "MSC_Group1");
        map.putSingle(TIME_QUERY_PARAM, THREE_HOURS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataMscGroupParamDayAgg() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, MSC_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, "MSC_Group1");
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataBscGroupParam() throws Exception {
        // MSS/NETWORK/INTERNAL_CAUSE_CODE_ANALYSIS?time=30&type=BSC&groupname=RNC_Group1&display=grid&tzOffset=+0100&maxRows=500
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, BSC_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, COUNT);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataBscGroupParam15MinAgg() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, BSC_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, THREE_HOURS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataBscGroupParamDayAgg() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, BSC_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, "RNC_Group1");
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataCellGroupParam() throws Exception {
        // MSS/NETWORK/INTERNAL_CAUSE_CODE_ANALYSIS?time=30&type=CELL&groupname=RNCGroup1&display=grid&tzOffset=+0100&maxRows=500
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, CELL_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, "RNCGroup1");
        map.putSingle(TIME_QUERY_PARAM, COUNT);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataCellGroupParam15MinAgg() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, CELL_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, "RNCGroup1");
        map.putSingle(TIME_QUERY_PARAM, THREE_HOURS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetCauseCodeAnalysisDataCellGroupParamDayAgg() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, CELL_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, "RNCGroup1");
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssCauseCodeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

}
