package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MssEventVolumeResourceIntegrationTest extends MssDataServiceBaseTestCase {
    private MultivaluedMap<String, String> map;

    private MssEventVolumeResource mssEventVolumeResource;

    private static final String DISPLAY_TYPE = CHART_PARAM;

    private static final String TIME = "time";

    private static final String TIME_5_MINS = "5";//TR1 (RAW Tables)

    private static final String TIME_30_MINS = "30";//TR1 (RAW Tables)

    private static final String TIME_26_HOURS = "1560";//TR3 (15MIN --> Aggregation table)

    private static final String TIME_15_DAYS = "21600";//TR4 (1DAY --> Aggregation table)

    private static final String MAX_ROWS_VALUE = "50";

    private static final String MSS_NAME_FOR_TEST = "MSS_1";

    private static final String MSS_GROUP_FOR_TEST = "MSS_Group1";

    private static final String CONTROLLER_NAME_FOR_TEST = "ONRM_RootMo_R:RNC01:RNC01,ERICSSON,1";

    private static final String CONTROLLER_GROUP_FOR_TEST = "RNC_Group1";

    private static final String CELL_NAME_FOR_TEST = "100,,ONRM_RootMo_R:RNC01:RNC01,Ericsson,1";

    private static final String CELL_GROUP_FOR_TEST = "RNCGroup1";

    @Override
    public void onSetUp() {
        mssEventVolumeResource = new MssEventVolumeResource();
        attachDependenciesForMssBaseResource(mssEventVolumeResource);
        mssEventVolumeResource.queryConstructor = this.queryConstructor;
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testNoDisplay() {
        map.clear();
        map.putSingle(TIME, TIME_5_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertTrue(result.contains(E_INVALID_OR_MISSING_PARAMS));
    }

    @Test
    public void testNotChartOrGridDisplay() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE + "1");
        map.putSingle(TIME, TIME_5_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertTrue(result.contains(E_NO_SUCH_DISPLAY_TYPE));
    }

    @Test
    public void testInvalidParametersHasTypeWithoutNodeOrGroup() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_5_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, MSC_PARAM.toUpperCase());
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertTrue(result.contains(E_INVALID_VALUES));
    }

    @Test
    public void testInvalidParametersInvalidType() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_5_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, "MSS_1".toUpperCase());
        map.putSingle(NODE_PARAM, "MSS_1");
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertTrue(result.contains(E_INVALID_VALUES));
    }

    @Test
    public void testInvalidParametersInvalidNode() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_5_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, MSC_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, "MSS@1");
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertTrue(result.contains(E_INVALID_VALUES));
    }

    @Test
    public void testGetEventVolumeDataForNoTypeRAW() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_5_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForNoType15MIN() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_26_HOURS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForNoTypeDAY() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_15_DAYS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForMSCRaw() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_30_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, MSC_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, MSS_NAME_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForMSCDAY() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_15_DAYS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, MSC_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, MSS_NAME_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForMSC15MIN() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_26_HOURS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, MSC_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, MSS_NAME_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForMSCGroupRaw() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_30_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, MSC_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, MSS_GROUP_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForMSCGroup15MIN() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_26_HOURS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, MSC_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, MSS_GROUP_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForMSCGroupDAY() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_15_DAYS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, MSC_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, MSS_GROUP_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForBSCRaw() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_30_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, BSC_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, CONTROLLER_NAME_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForBSC15MIN() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_26_HOURS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, BSC_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, CONTROLLER_NAME_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForBSCDAY() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_15_DAYS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, BSC_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, CONTROLLER_NAME_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForBSCGroupRaw() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_30_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, BSC_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, CONTROLLER_GROUP_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForBSCGroupDAY() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_15_DAYS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, BSC_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, CONTROLLER_GROUP_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForBSCGroup15MIN() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_26_HOURS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, BSC_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, CONTROLLER_GROUP_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForCELLRaw() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_30_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, CELL_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, CELL_NAME_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForCELL15MIN() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_26_HOURS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, CELL_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, CELL_NAME_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForCELLDAY() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_15_DAYS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, CELL_PARAM.toUpperCase());
        map.putSingle(NODE_PARAM, CELL_NAME_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForCELLGroupDay() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_15_DAYS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, CELL_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, CELL_GROUP_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForCELLGroup15MIN() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_26_HOURS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, CELL_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, CELL_GROUP_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetEventVolumeDataForCELLGroupRaw() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME, TIME_30_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, CELL_PARAM.toUpperCase());
        map.putSingle(GROUP_NAME_PARAM, CELL_GROUP_FOR_TEST);
        final String result = mssEventVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }
}
