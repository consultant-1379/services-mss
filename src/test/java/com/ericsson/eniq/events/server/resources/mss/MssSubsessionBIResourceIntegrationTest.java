package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MssSubsessionBIResourceIntegrationTest extends MssDataServiceBaseTestCase {
    private MultivaluedMap<String, String> map;

    private MssSubsessionBIResource mssSubsessionBIResource;

    private static final String TEST_TIME = "10080";

    private static final String TEST_GROUP_NAME = "IMSIGroup1";

    private static final String TEST_IMSI = "460000018556816";

    private static final String TEST_TZ_OFFSET = "+0100";

    private static final String TEST_MAX_ROWS = "50";

    @Override
    public void onSetUp() {

        mssSubsessionBIResource = new MssSubsessionBIResource();
        mssSubsessionBIResource.queryConstructor = this.queryConstructor;
        attachDependenciesForMssBaseResource(mssSubsessionBIResource);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetSubBIBusyDayData() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayData_DrillDown() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Friday");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayData_DrillDown_ERR_MsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayData_DrillDown_ERR_CallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(EVENT_ID_PARAM, "2");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayData_DrillDown_ERR_RoamingCallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(EVENT_ID_PARAM, "3");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayData_DrillDown_ERR_SMSMsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayData_DrillDown_ERR_LocationServices() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByIMSI_DrillDown() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Friday");
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByIMSI_DrillDown_ERR_MsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByIMSI_DrillDown_ERR_CallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(EVENT_ID_PARAM, "2");
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByIMSI_DrillDown_ERR_RoamingCallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(EVENT_ID_PARAM, "3");
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByIMSI_DrillDown_ERR_SMSMsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByIMSI_DrillDown_ERR_LocationServices() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourData() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourData_DrillDown() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourData_DrillDown_ERR_MsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourData_DrillDown_ERR_CallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(EVENT_ID_PARAM, "2");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourData_DrillDown_ERR_RoamingCallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(EVENT_ID_PARAM, "3");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourData_DrillDown_ERR_SMSOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourData_DrillDown_ERR_LocationService() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByIMSI_DrillDown() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, "460000123456790");
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByIMSI_DrillDown_ERR_MsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByIMSI_DrillDown_ERR_CallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(EVENT_ID_PARAM, "2");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByIMSI_DrillDown_ERR_RoamingCallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(EVENT_ID_PARAM, "3");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByIMSI_DrillDown_ERR_SMSOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByIMSI_DrillDown_ERR_LocationService() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByGroup_DrillDown_CallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "callForwarding,2");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByIMSI_DrillDown_CallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "callForwarding,2");
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByGroup_DrillDown_RoamingCall() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "roamingCallForwarding,3");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByIMSI_DrillDown_RoamingCall() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "roamingCallForwarding,3");
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByGroup_DrillDown_MsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "mSOriginating,0");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByIMSI_DrillDown_MsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "mSOriginating,0");
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByGroup_DrillDown_MsTerminating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "mSTerminating,1");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByIMSI_DrillDown_MsTerminating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "mSTerminating,1");
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBISubscriberDetailsData() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBISubscriberDetailsData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000082162865");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByMSISDN_DrillDown() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByMSISDN_DrillDown_ERR_MsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByMSISDN_DrillDown_ERR_CallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(EVENT_ID_PARAM, "2");
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByMSISDN_DrillDown_ERR_RoamingCallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(EVENT_ID_PARAM, "3");
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByMSISDN_DrillDown_ERR_SMSMsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByMSISDN_DrillDown_ERR_LocationServices() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(DAY_PARAM, "Thursday");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByMSISDN_DrillDown() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByMSISDN_DrillDown_ERR_MsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "2800");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByMSISDN_DrillDown_ERR_CallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "2800");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(EVENT_ID_PARAM, "2");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByMSISDN_DrillDown_ERR_RoamingCallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "2800");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(EVENT_ID_PARAM, "3");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByMSISDN_DrillDown_ERR_SMSMsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "2800");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByMSISDN_DrillDown_ERR_LocationServices() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "2800");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(HOUR_PARAM, "1");
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000082162865");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBICellAnalysisDataByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000082162865");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBICellAnalysisData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBICellAnalysisDataByMSISDN_DrillDown() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(NODE_PARAM, "CELL47312,,BSC237,Ericsson,GSM");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBICellAnalysisData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBICellAnalysisDataByMSISDN_DrillDown_ERR_MsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(CELL_SQL_ID, "7835244637733950438");
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBICellAnalysisData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBICellAnalysisDataByMSISDN_DrillDown_ERR_SMSOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(CELL_SQL_ID, "7835244637733950438");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBICellAnalysisData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBICellAnalysisDataByMSISDN_DrillDown_ERR_LocationServices() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(CELL_SQL_ID, "7835244637733950438");
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBICellAnalysisData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITerminalAnalysisDataByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBITerminalAnalysisData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITerminalAnalysisDataByMSISDN_DrillDown_ERR_MsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TAC_PARAM, "33000253");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBITerminalAnalysisData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITerminalAnalysisDataByMSISDN_DrillDown_ERR_RoamingCallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(EVENT_ID_PARAM, "3");
        map.putSingle(TAC_PARAM, "33000253");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBITerminalAnalysisData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITerminalAnalysisDataByMSISDN_DrillDown_ERR_SMSMsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TAC_PARAM, "33000253");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBITerminalAnalysisData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITerminalAnalysisDataByMSISDN_DrillDown_ERR_LocationServices() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000123457143");
        map.putSingle(EVENT_ID_PARAM, "6");
        map.putSingle(TAC_PARAM, "33000253");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBITerminalAnalysisData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByMSISDN_DrillDown_CallForwarding() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "callForwarding,2");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000082162865");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByMSISDN_DrillDown_RoamingCall() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "roamingCallForwarding,3");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000082162865");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByMSISDN_DrillDown_MsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "mSOriginating,0");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000082162865");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByMSISDN_DrillDown_MsTerminating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "mSTerminating,1");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000082162865");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByMSISDN_DrillDown_LocationServices() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "locationServices,6");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000082162865");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByMSISDN_DrillDown_SmsOriginating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "mSOriginatingSMSinMSC,4");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000082162865");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByMSISDN_DrillDown_SmsTerminating() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(EVENT_NAME_PARAM, "mSTerminatingSMSinMSC,5");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, "460000082162865");
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testMissingDisplayParam() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfoForMss(map, mssSubsessionBIResource, SAMPLE_BASE_URI, "somePath");
        final String result = mssSubsessionBIResource.getSubBIFailureData();
        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }
}
