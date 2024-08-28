package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Ignore;
import org.junit.Test;

import com.sun.jersey.core.util.MultivaluedMapImpl;

@Ignore
public class MssKPIResourceIntegrationTest extends MssDataServiceBaseTestCase {

    private static final String TIME_VALUE_GREATER_THAN_TWO_WEEK = "20170";

    private static final String TIME_VALUE_ONE_DAY = "1440";

    private MultivaluedMap<String, String> map;

    private MssKPIResource mssKPIResource;

    private static final String DISPLAY_TYPE = CHART_PARAM;

    private static final String TIME = "30";

    private static final String TIME_FROM = "1500";

    private static final String TIME_TO = "1600";

    private static final String DATE_FROM = "11052010";

    private static final String DATE_TO = "12052010";

    private static final String NODE = "ONRM_RootMo_R:RNC01:RNC01,ERICSSON,1";

    private static final String TIME_VALUE_FIVE_MINUTES = "5";

    private static final String MAX_ROWS_VALUE = "50";

    @Override
    public void onSetUp() {
        mssKPIResource = new MssKPIResource();
        attachDependenciesForMssBaseResource(mssKPIResource);
        mssKPIResource.mssQueryBuilder = mssQueryFactoryHelper;
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetDataByTime_Manufacturer_5Minutes() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle("manufacturer", "Ningbo%20Bird%20Co%20Ltd%20%2899%20Chenhshan%20/999%20Dachen%29");
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetDataByTime_Manufacturer_1Day() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle("manufacturer", "Ningbo%20Bird%20Co%20Ltd%20%2899%20Chenhshan%20/999%20Dachen%29");
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetDataByTime_Manufacturer_2Week() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_TWO_WEEK);
        map.putSingle("manufacturer", "Ningbo%20Bird%20Co%20Ltd%20%2899%20Chenhshan%20/999%20Dachen%29");
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_TAC_5_minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_TAC_1_day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_TAC_2_week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_TWO_WEEK);
        map.putSingle(TAC_PARAM, "10052026");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_BSC_5_Minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_BSC_30_Minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_BSC_1_Day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_BSC_2_Week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_TWO_WEEK);
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_CELL_30_Minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(NODE_PARAM, "CELL3000,,BSC1,ERICSSON,1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_CELL_1_DAY() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(NODE_PARAM, "CELL3000,,BSC1,ERICSSON,1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_CELL_2_Week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_TWO_WEEK);
        map.putSingle(NODE_PARAM, "CELL3000,,BSC1,ERICSSON,1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_MSC_30_Minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(NODE_PARAM, "MSC1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_MSC_1_DAY() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(NODE_PARAM, "MSC1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_MSC_2_Week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_TWO_WEEK);
        map.putSingle(NODE_PARAM, "MSC1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_TACGroup_5_minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_ExclusiveTACGroup_5_minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(GROUP_NAME_PARAM, EXCLUSIVE_TAC_GROUP_NAME);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_TACGroup_1_day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_TACGroup_2_week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_TWO_WEEK);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_BSCGroup_5_Minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_BSCGroup_30_Minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_BSCGroup_1_Day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_BSCGroup_2_Week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_TWO_WEEK);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_CELLGroup_30_Minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_CELLGroup_1_Day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_CELLGroup_2_Week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_TWO_WEEK);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_MscGroup_30_Minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_MscGroup_1_Day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_MscGroup_2_Week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_GREATER_THAN_TWO_WEEK);
        map.putSingle(GROUP_NAME_PARAM, "tempGrp");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = mssKPIResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void verifyDisplayType() throws Exception {
        final String invalidDisplayType = "error";

        map.clear();
        map.putSingle(DISPLAY_PARAM, invalidDisplayType);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssKPIResource.getData("requestID", map);
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
        final String result = mssKPIResource.getData("requestID", map);
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
        final String result = mssKPIResource.getData("requestID", map);
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
        final String result = mssKPIResource.getData("requestID", map);
        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }

    @Test
    public void testCheckValidBSC() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(NODE_PARAM, "dfasdaf");
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssKPIResource.getData("requestID", map);
        assertJSONErrorResult(result);
    }

}
