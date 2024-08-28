/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.eniq.events.server.integritytests.mss.eventanalysis;

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

import org.junit.AfterClass;
import org.junit.Test;

import com.ericsson.eniq.events.server.integritytests.mss.MssTestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.resources.mss.MssEventAnalysisResource;
import com.ericsson.eniq.events.server.test.database.DatabaseConnectionHelper;
import com.ericsson.eniq.events.server.test.queryresults.MssAccessAreaEventAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.queryresults.MssControllerEventAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.queryresults.MssEventAnalysisDetailedResult;
import com.ericsson.eniq.events.server.test.queryresults.MssImsiMsisdnEventAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.queryresults.MssMscEventAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.queryresults.MssTacAndAllGroupsEventAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.sql.SQLCommand;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MssEventAnalysisWithPreparedTablesForAllNodesTypesTest extends MssTestsWithTemporaryTablesBaseTestCase<MssEventAnalysisDetailedResult> {

    private final MssEventAnalysisResource mssEventAnalysisResource = new MssEventAnalysisResource();

    private final static Map<String, Object> constantcolumnsInMssAllSchemas = new HashMap<String, Object>();

    private final static Map<String, Object> constantcolumnsInMssAllSchemasDay = new HashMap<String, Object>();

    private final static Map<String, Object> constantcolumnsInMssVoiceRawTables = new HashMap<String, Object>();

    private final static Map<String, Object> constantcolumnsInMssVoiceDayTables = new HashMap<String, Object>();

    private final static Map<String, Object> constantcolumnsInMssLocationRawTables = new HashMap<String, Object>();

    private final static Map<String, Object> constantcolumnsInMssLocationDayTables = new HashMap<String, Object>();

    private final static Map<String, Object> constantcolumnsInMssSmsRawTables = new HashMap<String, Object>();

    private final static Map<String, Object> constantcolumnsInMssSmsDayTables = new HashMap<String, Object>();

    private final static List<String> tempErrRawTablesVoice = new ArrayList<String>();

    private final static List<String> tempErrRawTablesLocationService = new ArrayList<String>();

    private final static List<String> tempErrRawTablesSms = new ArrayList<String>();

    private final static List<String> tempErrDayTablesVoice = new ArrayList<String>();

    private final static List<String> tempErrDayTablesSms = new ArrayList<String>();

    private final static List<String> tempErrDayTablesLocService = new ArrayList<String>();

    private final static String NULL_VALUE = "NULL";

    static {

        tempErrRawTablesVoice.add(TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW);
        tempErrRawTablesVoice.add(TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW);

        tempErrDayTablesVoice.add(TEMP_VOICE_EVENT_SRC_SUM_ERR_DAY);
        tempErrDayTablesVoice.add(TEMP_VOICE_EVENT_SRC_SUM_DROP_DAY);
        tempErrRawTablesLocationService.add(TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_ERR_RAW);

        tempErrDayTablesLocService.add(TEMP_LOC_EVENT_SRC_SUM_ERR_DAY);
        tempErrRawTablesSms.add(TEMP_EVENT_E_MSS_SMS_CDR_ERR_RAW);
        tempErrDayTablesSms.add(TEMP_SMS_EVNTSRC_EVENTID_ERR_DAY);
    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        mssEventAnalysisResource.setQueryConstructor(this.queryConstructor);
        attachDependenciesForMSSBaseResource(mssEventAnalysisResource);
        final String timestamp = DateTimeUtilities.getDateTimeMinus5Minutes();
        final String timestampWeek = DateTimeUtilities.getDateTimeMinus36Hours();
        setupRawTableConstantValuesForMssDay(timestampWeek);
        setupRawTableConstantValuesForMss(timestamp);
        setupRawTableConstantValuesForVoice();
        setupRawTableConstantValuesForLocationService();
        setupRawTableConstantValuesForSms();
    }

    @Test
    public void testGetSummaryDataByTime_TAC_30_minutes() throws Exception {
        createNPopulateSummaryTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TAC_PARAM, TEST_VALUE_TAC);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssTacAndAllGroupsEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssTacAndAllGroupsEventAnalysisSummaryResult>()
                .translateResult(json, MssTacAndAllGroupsEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(6));
        for (final MssTacAndAllGroupsEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(result.getTacOrGroupName(), TEST_VALUE_TAC);
            if (MSS_LOCATION_SERVICE_EVENT_ID.equals(eventId) || isMssSMSEvent(eventId)) {
                assertEquals(result.getErrorCount(), "1");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            } else {
                assertEquals(result.getErrorCount(), "2");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            }
        }
    }

    @Test
    public void testGetSummaryDataByTime_MSC_30_minutes() throws Exception {
        createNPopulateSummaryTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(NODE_PARAM, TEST_VALUE_MSS_EVNTSRC);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);
        final String json = mssEventAnalysisResource.getData();
        final List<MssMscEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssMscEventAnalysisSummaryResult>().translateResult(json,
                MssMscEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(7));
        for (final MssMscEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(result.getMsc(), TEST_VALUE_MSS_EVNTSRC);
            assertEquals(Long.parseLong(result.getEvntSrcId()), TEST_VALUE_MSS_EVNTSRC_ID);
            if (MSS_LOCATION_SERVICE_EVENT_ID.equals(eventId) || isMssSMSEvent(eventId)) {
                assertEquals(result.getErrorCount(), "1");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            } else {
                assertEquals(result.getErrorCount(), "2");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            }
        }
    }

    @Test
    public void testGetEventAnalysisSummaryDataByTime_MSC_VOICE_ONE_Week() throws Exception {
        createNPopulateSummaryTablesWeek();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(NODE_PARAM, TEST_VALUE_MSS_EVNTSRC);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_FIVE_THIRTY);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(FAIL_TYPE, BLOCKED_VALUE);
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssMscEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssMscEventAnalysisSummaryResult>().translateResult(json,
                MssMscEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(4));
        for (final MssMscEventAnalysisSummaryResult result : summaryResult) {
            assertEquals(result.getMsc(), TEST_VALUE_MSS_EVNTSRC);
            assertEquals(Long.parseLong(result.getEvntSrcId()), TEST_VALUE_MSS_EVNTSRC_ID);
            assertEquals(result.getErrorCount(), "2");
            assertEquals(result.getSuccessCount(), "2");
            assertEquals(result.getErrorSubscriberCount(), "1");
        }
    }

    @Test
    public void testGetEventAnalysisSummaryDataByTime_MSC_LOCSERVICE_ONE_Week() throws Exception {
        createNPopulateErrTempTablesLocServicesWeek(this.connection);
        createNPopulateSucTempTablesLocServicesWeek(this.connection);
        createNPopulateErrTempTablesLocServices(this.connection);
        createNPopulateSucTempTablesLocServices(this.connection);
        commonTemporaryTopologyTables(this.connection);
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_EVNTSRC);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssMscEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssMscEventAnalysisSummaryResult>().translateResult(json,
                MssMscEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssMscEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_LOCATION_SERVICE_EVENT_ID, eventId);
        }
    }

    @Test
    public void testGetEventAnalysisSummaryDataByTime_MSC_SMS_ONE_Week() throws Exception {
        createNPopulateErrTempTablesSmsWeek(this.connection);
        createNPopulateSucTempTablesSmsWeek(this.connection);
        createNPopulateErrTempTablesSms(this.connection);
        createNPopulateSucTempTablesSms(this.connection);
        commonTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, EXCLUSIVE_TAC_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssTacAndAllGroupsEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssTacAndAllGroupsEventAnalysisSummaryResult>()
                .translateResult(json, MssTacAndAllGroupsEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssTacAndAllGroupsEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_SMS_MS_ORIGINATING_EVENT_ID, eventId);
            assertTrue(TEST_VALUE_EXCLUSIVE_TAC.equals(result.getTacOrGroupName()));
        }
    }

    @Test
    public void testGetSummaryDataByTime_Controller30_Minutes() throws Exception {
        createNPopulateSummaryTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(NODE_PARAM, TEST_VALUE_MSS_CONTROLLER_NODE);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssControllerEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssControllerEventAnalysisSummaryResult>()
                .translateResult(json, MssControllerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(5));
        for (final MssControllerEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(result.getController(), TEST_VALUE_MSS_CONTROLLER_NAME);
            assertEquals(result.getVendor(), ERICSSON);
            assertEquals(Long.parseLong(result.getHier3Id()), TEST_VALUE_MSS_HIER3_ID);
            if (MSS_LOCATION_SERVICE_EVENT_ID.equals(eventId) || isMssSMSEvent(eventId)) {
                assertEquals(result.getErrorCount(), "1");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            } else {
                assertEquals(result.getErrorCount(), "2");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            }
        }
    }

    @Test
    public void testGetSummaryDataByTime_CELL30_Minutes() throws Exception {
        createNPopulateSummaryTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(NODE_PARAM, TEST_VALUE_MSS_CELL_NODE);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssAccessAreaEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssAccessAreaEventAnalysisSummaryResult>()
                .translateResult(json, MssAccessAreaEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(5));
        for (final MssAccessAreaEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(result.getAccessArea(), TEST_VALUE_MSS_CELL_NAME);
            assertEquals(result.getController(), TEST_VALUE_MSS_CONTROLLER_NAME);
            assertEquals(result.getVendor(), ERICSSON);
            assertEquals(Long.parseLong(result.getHier321Id()), TEST_VALUE_MSS_HIER321_ID);
            if (MSS_LOCATION_SERVICE_EVENT_ID.equals(eventId) || isMssSMSEvent(eventId)) {
                assertEquals(result.getErrorCount(), "1");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            } else {
                assertEquals(result.getErrorCount(), "2");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            }
        }
    }

    @Test
    public void testGetSummaryDataByTime_Imsi() throws Exception {
        createNPopulateSummaryTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_VALUE_IMSI));
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssImsiMsisdnEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssImsiMsisdnEventAnalysisSummaryResult>()
                .translateResult(json, MssImsiMsisdnEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(7));
        for (final MssImsiMsisdnEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(result.getImsiOrMsisdn(), Long.toString(TEST_VALUE_IMSI));
            if (MSS_LOCATION_SERVICE_EVENT_ID.equals(eventId) || isMssSMSEvent(eventId)) {
                assertEquals(result.getErrorCount(), "1");
                assertEquals(result.getSuccessCount(), "1");
            } else {
                assertEquals(result.getErrorCount(), "2");
                assertEquals(result.getSuccessCount(), "1");
            }
        }
    }

    @Test
    public void testGetSummaryDataByTime_MSISDN() throws Exception {
        createNPopulateSummaryTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, Long.toString(TEST_VALUE_MSISDN));
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssImsiMsisdnEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssImsiMsisdnEventAnalysisSummaryResult>()
                .translateResult(json, MssImsiMsisdnEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(7));
        for (final MssImsiMsisdnEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(result.getImsiOrMsisdn(), Long.toString(TEST_VALUE_MSISDN));
            if (MSS_LOCATION_SERVICE_EVENT_ID.equals(eventId) || isMssSMSEvent(eventId)) {
                assertEquals(result.getErrorCount(), "1");
                assertEquals(result.getSuccessCount(), "1");
            } else {
                assertEquals(result.getErrorCount(), "2");
                assertEquals(result.getSuccessCount(), "1");
            }
        }
    }

    @Test
    public void testGetSummaryDataByTime_TACGroup_30_minutes() throws Exception {
        createNPopulateSummaryTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_TAC_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssTacAndAllGroupsEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssTacAndAllGroupsEventAnalysisSummaryResult>()
                .translateResult(json, MssTacAndAllGroupsEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(6));
        for (final MssTacAndAllGroupsEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(result.getTacOrGroupName(), TEST_VALUE_TAC_GROUP);
            if (MSS_LOCATION_SERVICE_EVENT_ID.equals(eventId) || isMssSMSEvent(eventId)) {
                assertEquals(result.getErrorCount(), "1");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            } else {
                assertEquals(result.getErrorCount(), "2");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            }
        }
    }

    @Test
    public void testGetSummaryDataByTime_ExclusiveTACGroup_30_Min() throws Exception {
        createNPopulateSummaryTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, EXCLUSIVE_TAC_GROUP);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);
        final String json = mssEventAnalysisResource.getData();
        final List<MssTacAndAllGroupsEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssTacAndAllGroupsEventAnalysisSummaryResult>()
                .translateResult(json, MssTacAndAllGroupsEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(6));
        for (final MssTacAndAllGroupsEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(result.getTacOrGroupName(), EXCLUSIVE_TAC_GROUP);
            if (MSS_LOCATION_SERVICE_EVENT_ID.equals(eventId) || isMssSMSEvent(eventId)) {
                assertEquals(result.getErrorCount(), "1");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            } else {
                assertEquals(result.getErrorCount(), "2");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            }
        }
    }

    @Test
    public void testGetSummaryDataByTime_MSCGroup_30_minutes() throws Exception {
        createNPopulateGroupSummaryTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_EVNTSRC);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssTacAndAllGroupsEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssTacAndAllGroupsEventAnalysisSummaryResult>()
                .translateResult(json, MssTacAndAllGroupsEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(7));
        for (final MssTacAndAllGroupsEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(result.getTacOrGroupName(), TEST_VALUE_MSS_GROUP_EVNTSRC);
            if (MSS_LOCATION_SERVICE_EVENT_ID.equals(eventId) || isMssSMSEvent(eventId)) {
                assertEquals(result.getErrorCount(), "1");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            } else {
                assertEquals(result.getErrorCount(), "2");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            }
        }
    }

    @Test
    public void testGetSummaryDataByTime_ControllerGroup_30_Minutes() throws Exception {
        createNPopulateGroupSummaryTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CONTROLLER);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssTacAndAllGroupsEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssTacAndAllGroupsEventAnalysisSummaryResult>()
                .translateResult(json, MssTacAndAllGroupsEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(5));
        for (final MssTacAndAllGroupsEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(result.getTacOrGroupName(), TEST_VALUE_MSS_GROUP_CONTROLLER);
            if (MSS_LOCATION_SERVICE_EVENT_ID.equals(eventId) || isMssSMSEvent(eventId)) {
                assertEquals(result.getErrorCount(), "1");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            } else {
                assertEquals(result.getErrorCount(), "2");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            }
        }
    }

    @Test
    public void testGetSummaryDataByTime_CELLGroup30_Minutes() throws Exception {
        createNPopulateGroupSummaryTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CELL);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssTacAndAllGroupsEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssTacAndAllGroupsEventAnalysisSummaryResult>()
                .translateResult(json, MssTacAndAllGroupsEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(5));
        for (final MssTacAndAllGroupsEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(result.getTacOrGroupName(), TEST_VALUE_MSS_GROUP_CELL);
            if (MSS_LOCATION_SERVICE_EVENT_ID.equals(eventId) || isMssSMSEvent(eventId)) {
                assertEquals(result.getErrorCount(), "1");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            } else {
                assertEquals(result.getErrorCount(), "2");
                assertEquals(result.getSuccessCount(), "1");
                assertEquals(result.getErrorSubscriberCount(), "1");
            }
        }
    }

    @Test
    public void testGetSummaryDataByTime_TACGroupDrillDownVoice_30_minutes() throws Exception {
        createNPopulateErrTempTablesVoice(this.connection);
        createNPopulateSucTempTablesVoice(this.connection);
        commonTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_TAC_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssTacAndAllGroupsEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssTacAndAllGroupsEventAnalysisSummaryResult>()
                .translateResult(json, MssTacAndAllGroupsEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssTacAndAllGroupsEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_MS_ORIGINATING_EVENT_ID, eventId);
            assertTrue(!TEST_VALUE_EXCLUSIVE_TAC.equals(result.getTacOrGroupName()));
        }
    }

    @Test
    public void testGetSummaryDataByTime_ExclusiveTACGroupDrillDownVoice_30_minutes() throws Exception {
        createNPopulateErrTempTablesVoice(this.connection);
        createNPopulateSucTempTablesVoice(this.connection);
        commonTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, EXCLUSIVE_TAC_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssTacAndAllGroupsEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssTacAndAllGroupsEventAnalysisSummaryResult>()
                .translateResult(json, MssTacAndAllGroupsEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssTacAndAllGroupsEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_MS_ORIGINATING_EVENT_ID, eventId);
            //assertTrue(TEST_VALUE_EXCLUSIVE_TAC.equals(result.getTacOrGroupName()));
        }
    }

    @Test
    public void testGetSummaryDataByTime_MSCGroupDrillDownVoice_30_minutes() throws Exception {
        commonTemporaryTopologyTables(this.connection);
        createGroupTemporaryTopologyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createNPopulateSucTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_EVNTSRC);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssMscEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssMscEventAnalysisSummaryResult>().translateResult(json,
                MssMscEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssMscEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_MS_ORIGINATING_EVENT_ID, eventId);
        }
    }

    @Test
    public void testGetSummaryDataByTime_ControllerGroupDrillDownVoice_30_Minutes() throws Exception {
        commonTemporaryTopologyTables(this.connection);
        createGroupTemporaryTopologyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createNPopulateSucTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CONTROLLER);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssControllerEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssControllerEventAnalysisSummaryResult>()
                .translateResult(json, MssControllerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssControllerEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_MS_ORIGINATING_EVENT_ID, eventId);
        }
    }

    @Test
    public void testGetSummaryDataByTime_CELLGroupDrillDownVoice() throws Exception {
        commonTemporaryTopologyTables(this.connection);
        createGroupTemporaryTopologyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createNPopulateSucTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CELL);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssAccessAreaEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssAccessAreaEventAnalysisSummaryResult>()
                .translateResult(json, MssAccessAreaEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssAccessAreaEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_MS_ORIGINATING_EVENT_ID, eventId);
        }
    }

    @Test
    public void testGetSummaryDataByTime_TACGroupDrillDownLocationService_30_minutes() throws Exception {
        createNPopulateErrTempTablesLocServices(this.connection);
        createNPopulateSucTempTablesLocServices(this.connection);
        commonTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_TAC_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssTacAndAllGroupsEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssTacAndAllGroupsEventAnalysisSummaryResult>()
                .translateResult(json, MssTacAndAllGroupsEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssTacAndAllGroupsEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_LOCATION_SERVICE_EVENT_ID, eventId);
            assertTrue(!TEST_VALUE_EXCLUSIVE_TAC.equals(result.getTacOrGroupName()));
        }
    }

    @Test
    public void testGetSummaryDataByTime_ExclusiveTACGroupDrillDownLocationService_30_minutes() throws Exception {
        createNPopulateErrTempTablesLocServices(this.connection);
        createNPopulateSucTempTablesLocServices(this.connection);
        commonTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, EXCLUSIVE_TAC_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssTacAndAllGroupsEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssTacAndAllGroupsEventAnalysisSummaryResult>()
                .translateResult(json, MssTacAndAllGroupsEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssTacAndAllGroupsEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_LOCATION_SERVICE_EVENT_ID, eventId);
            assertTrue(TEST_VALUE_EXCLUSIVE_TAC.equals(result.getTacOrGroupName()));
        }
    }

    @Test
    public void testGetSummaryDataByTime_MSCGroupDrillDownLocationService_30_minutes() throws Exception {
        commonTemporaryTopologyTables(this.connection);
        createGroupTemporaryTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices(this.connection);
        createNPopulateSucTempTablesLocServices(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_EVNTSRC);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssMscEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssMscEventAnalysisSummaryResult>().translateResult(json,
                MssMscEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssMscEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_LOCATION_SERVICE_EVENT_ID, eventId);
        }
    }

    @Test
    public void testGetSummaryDataByTime_ControllerGroupDrillDownLocationService_30_Minutes() throws Exception {
        commonTemporaryTopologyTables(this.connection);
        createGroupTemporaryTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices(this.connection);
        createNPopulateSucTempTablesLocServices(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CONTROLLER);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssControllerEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssControllerEventAnalysisSummaryResult>()
                .translateResult(json, MssControllerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssControllerEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_LOCATION_SERVICE_EVENT_ID, eventId);
        }
    }

    @Test
    public void testGetSummaryDataByTime_CELLGroupDrillDownLocationService() throws Exception {
        commonTemporaryTopologyTables(this.connection);
        createGroupTemporaryTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices(this.connection);
        createNPopulateSucTempTablesLocServices(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CELL);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssAccessAreaEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssAccessAreaEventAnalysisSummaryResult>()
                .translateResult(json, MssAccessAreaEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssAccessAreaEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_LOCATION_SERVICE_EVENT_ID, eventId);
        }
    }

    @Test
    public void testGetSummaryDataByTime_TACGroupDrillDownSms_30_minutes() throws Exception {
        createNPopulateErrTempTablesSms(this.connection);
        createNPopulateSucTempTablesSms(this.connection);
        commonTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_TAC_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssTacAndAllGroupsEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssTacAndAllGroupsEventAnalysisSummaryResult>()
                .translateResult(json, MssTacAndAllGroupsEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssTacAndAllGroupsEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_SMS_MS_ORIGINATING_EVENT_ID, eventId);
            assertTrue(!TEST_VALUE_EXCLUSIVE_TAC.equals(result.getTacOrGroupName()));
        }
    }

    @Test
    public void testGetSummaryDataByTime_ExclusiveTACGroupDrillDownSms_30_minutes() throws Exception {
        createNPopulateErrTempTablesSms(this.connection);
        createNPopulateSucTempTablesSms(this.connection);
        commonTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, EXCLUSIVE_TAC_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssTacAndAllGroupsEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssTacAndAllGroupsEventAnalysisSummaryResult>()
                .translateResult(json, MssTacAndAllGroupsEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssTacAndAllGroupsEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_SMS_MS_ORIGINATING_EVENT_ID, eventId);
            assertTrue(TEST_VALUE_EXCLUSIVE_TAC.equals(result.getTacOrGroupName()));
        }
    }

    @Test
    public void testGetSummaryDataByTime_MSCGroupDrillDownSms_30_minutes() throws Exception {
        commonTemporaryTopologyTables(this.connection);
        createGroupTemporaryTopologyTables(this.connection);
        createNPopulateErrTempTablesSms(this.connection);
        createNPopulateSucTempTablesSms(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_EVNTSRC);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssMscEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssMscEventAnalysisSummaryResult>().translateResult(json,
                MssMscEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssMscEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_SMS_MS_ORIGINATING_EVENT_ID, eventId);
        }
    }

    @Test
    public void testGetSummaryDataByTime_ControllerGroupDrillDownSms_30_Minutes() throws Exception {
        commonTemporaryTopologyTables(this.connection);
        createGroupTemporaryTopologyTables(this.connection);
        createNPopulateErrTempTablesSms(this.connection);
        createNPopulateSucTempTablesSms(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CONTROLLER);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssControllerEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssControllerEventAnalysisSummaryResult>()
                .translateResult(json, MssControllerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssControllerEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_SMS_MS_ORIGINATING_EVENT_ID, eventId);
        }
    }

    @Test
    public void testGetSummaryDataByTime_CELLGroupDrillDownSms() throws Exception {
        commonTemporaryTopologyTables(this.connection);
        createGroupTemporaryTopologyTables(this.connection);
        createNPopulateErrTempTablesSms(this.connection);
        createNPopulateSucTempTablesSms(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CELL);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssAccessAreaEventAnalysisSummaryResult> summaryResult = new ResultTranslator<MssAccessAreaEventAnalysisSummaryResult>()
                .translateResult(json, MssAccessAreaEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));
        for (final MssAccessAreaEventAnalysisSummaryResult result : summaryResult) {
            final String eventId = result.getEventId();
            assertEquals(MSS_SMS_MS_ORIGINATING_EVENT_ID, eventId);
        }
    }

    @Test
    public void testGetDetailedData_IMSI_30min_MsOriginating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_VALUE_IMSI));
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING.equals(rowData.getEventIdDesc()));
            assertTrue(Long.toString(TEST_VALUE_IMSI).equals(rowData.getImsi()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_IMSI_30min_MsTerminating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_VALUE_IMSI));
        map.putSingle(EVENT_ID_PARAM, MSS_MS_TERMINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_TERMINATING.equals(rowData.getEventIdDesc()));
            assertTrue(Long.toString(TEST_VALUE_IMSI).equals(rowData.getImsi()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_IMSI_30min_CallForwarding() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_VALUE_IMSI));
        map.putSingle(EVENT_ID_PARAM, MSS_CALL_FORWARDING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(CALL_FORWARDING.equals(rowData.getEventIdDesc()));
            assertTrue(Long.toString(TEST_VALUE_IMSI).equals(rowData.getImsi()));
            assertTrue(rowData.getTac() == null || rowData.getTac().length() == 0);
            assertNull(rowData.getCell());
            assertNull(rowData.getController());
            assertNull(rowData.getRatDesc());
        }
    }

    @Test
    public void testGetDetailedData_IMSI_30min_RoamingCallForwarding() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_VALUE_IMSI));
        map.putSingle(EVENT_ID_PARAM, MSS_ROAMING_CALL_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(ROAMING_CALL_FORWARDING.equals(rowData.getEventIdDesc()));
            assertTrue(Long.toString(TEST_VALUE_IMSI).equals(rowData.getImsi()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
            assertNull(rowData.getCell());
            assertNull(rowData.getController());
            assertNull(rowData.getRatDesc());
        }
    }

    @Test
    public void testGetDetailedData_IMSI_30min_LocationService() throws Exception {
        createLocServiceTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_VALUE_IMSI));
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(LOCATION_SERVICES.equals(rowData.getEventIdDesc()));
            assertTrue(Long.toString(TEST_VALUE_IMSI).equals(rowData.getImsi()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_IMSI_30min_MsOriginatingSMS() throws Exception {
        createSmsTopologyTables(this.connection);
        createNPopulateErrTempTablesSms();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_VALUE_IMSI));
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING_SMS_MSC.equals(rowData.getEventIdDesc()));
            assertTrue(Long.toString(TEST_VALUE_IMSI).equals(rowData.getImsi()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
            assertTrue(Long.toString(TEST_VALUE_CALLING_SUB_IMEISV).equals(rowData.getCallingSubImeiSv()));
            assertTrue(Long.toString(TEST_VALUE_CALLED_SUB_IMEISV).equals(rowData.getCalledSubImeiSv()));
        }
    }

    @Test
    public void testGetDetailedData_MSISDN_30min_MsOriginating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, Long.toString(TEST_VALUE_MSISDN));
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING.equals(rowData.getEventIdDesc()));
            assertTrue(Long.toString(TEST_VALUE_MSISDN).equals(rowData.getImsi()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }

    }

    @Test
    public void testGetDetailedData_MSISDN_30min_MsTerminating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, Long.toString(TEST_VALUE_MSISDN));
        map.putSingle(EVENT_ID_PARAM, MSS_MS_TERMINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_TERMINATING.equals(rowData.getEventIdDesc()));
            assertTrue(Long.toString(TEST_VALUE_MSISDN).equals(rowData.getImsi()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_MSISDN_30min_CallForwarding() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, Long.toString(TEST_VALUE_MSISDN));
        map.putSingle(EVENT_ID_PARAM, MSS_CALL_FORWARDING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(CALL_FORWARDING.equals(rowData.getEventIdDesc()));
            assertTrue(Long.toString(TEST_VALUE_MSISDN).equals(rowData.getImsi()));
            assertTrue(rowData.getTac() == null || rowData.getTac().length() == 0);
            assertNull(rowData.getCell());
            assertNull(rowData.getController());
            assertNull(rowData.getRatDesc());
        }
    }

    @Test
    public void testGetDetailedData_MSISDN_30min_RoamingCallForwarding() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, Long.toString(TEST_VALUE_MSISDN));
        map.putSingle(EVENT_ID_PARAM, MSS_ROAMING_CALL_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(ROAMING_CALL_FORWARDING.equals(rowData.getEventIdDesc()));
            assertTrue(Long.toString(TEST_VALUE_MSISDN).equals(rowData.getImsi()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
            assertNull(rowData.getCell());
            assertNull(rowData.getController());
            assertNull(rowData.getRatDesc());
        }
    }

    @Test
    public void testGetDetailedData_MSISDN_30min_LocationService() throws Exception {
        createLocServiceTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, Long.toString(TEST_VALUE_MSISDN));
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(LOCATION_SERVICES.equals(rowData.getEventIdDesc()));
            assertTrue(Long.toString(TEST_VALUE_MSISDN).equals(rowData.getImsi()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_MSISDN_30min_MsOriginatingSMS() throws Exception {
        createSmsTopologyTables(this.connection);
        createNPopulateErrTempTablesSms();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, Long.toString(TEST_VALUE_MSISDN));
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING_SMS_MSC.equals(rowData.getEventIdDesc()));
            assertTrue(Long.toString(TEST_VALUE_MSISDN).equals(rowData.getImsi()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
            assertTrue(Long.toString(TEST_VALUE_CALLING_SUB_IMEISV).equals(rowData.getCallingSubImeiSv()));
            assertTrue(Long.toString(TEST_VALUE_CALLED_SUB_IMEISV).equals(rowData.getCalledSubImeiSv()));
        }
    }

    @Test
    public void testGetDetailedData_MSC_30min_MsOriginating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, Long.toString(TEST_VALUE_MSS_EVNTSRC_ID));
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_MSS_EVNTSRC.equals(rowData.getEventSourceName()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }

    }

    @Test
    public void testGetDetailedData_MSC_30min_MsTerminating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, Long.toString(TEST_VALUE_MSS_EVNTSRC_ID));
        map.putSingle(EVENT_ID_PARAM, MSS_MS_TERMINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_TERMINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_MSS_EVNTSRC.equals(rowData.getEventSourceName()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_MSC_30min_CallForwarding() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, Long.toString(TEST_VALUE_MSS_EVNTSRC_ID));
        map.putSingle(EVENT_ID_PARAM, MSS_CALL_FORWARDING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(CALL_FORWARDING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_MSS_EVNTSRC.equals(rowData.getEventSourceName()));
            assertTrue(rowData.getTac() == null || rowData.getTac().length() == 0);
            assertNull(rowData.getCell());
            assertNull(rowData.getController());
            assertNull(rowData.getRatDesc());
        }
    }

    @Test
    public void testGetDetailedData_MSC_30min_RoamingCallForwarding() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, Long.toString(TEST_VALUE_MSS_EVNTSRC_ID));
        map.putSingle(EVENT_ID_PARAM, MSS_ROAMING_CALL_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(ROAMING_CALL_FORWARDING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_MSS_EVNTSRC.equals(rowData.getEventSourceName()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
            assertNull(rowData.getCell());
            assertNull(rowData.getController());
            assertNull(rowData.getRatDesc());
        }
    }

    @Test
    public void testGetDetailedData_MSC_30min_LocationService() throws Exception {
        createLocServiceTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, Long.toString(TEST_VALUE_MSS_EVNTSRC_ID));
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(LOCATION_SERVICES.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_MSS_EVNTSRC.equals(rowData.getEventSourceName()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_MSC_30min_MsOriginatingSMS() throws Exception {
        createSmsTopologyTables(this.connection);
        createNPopulateErrTempTablesSms();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(EVENT_SOURCE_SQL_ID, Long.toString(TEST_VALUE_MSS_EVNTSRC_ID));
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING_SMS_MSC.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_MSS_EVNTSRC.equals(rowData.getEventSourceName()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_CONTROLLER_30min_MsOriginating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(CONTROLLER_SQL_ID, Long.toString(TEST_VALUE_MSS_HIER3_ID));
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_MSS_CONTROLLER_NAME.equals(rowData.getController()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }

    }

    @Test
    public void testGetDetailedData_CONTROLLER_30min_MsTerminating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(CONTROLLER_SQL_ID, Long.toString(TEST_VALUE_MSS_HIER3_ID));
        map.putSingle(EVENT_ID_PARAM, MSS_MS_TERMINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_TERMINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_MSS_CONTROLLER_NAME.equals(rowData.getController()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_CONTROLLER_30min_LocationService() throws Exception {
        createLocServiceTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(CONTROLLER_SQL_ID, Long.toString(TEST_VALUE_MSS_HIER3_ID));
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(LOCATION_SERVICES.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_MSS_CONTROLLER_NAME.equals(rowData.getController()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_CONTROLLER_30min_MsOriginatingSMS() throws Exception {
        createSmsTopologyTables(this.connection);
        createNPopulateErrTempTablesSms();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(CONTROLLER_SQL_ID, Long.toString(TEST_VALUE_MSS_HIER3_ID));
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING_SMS_MSC.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_MSS_CONTROLLER_NAME.equals(rowData.getController()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_CELL_30min_MsOriginating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(CELL_SQL_ID, Long.toString(TEST_VALUE_MSS_HIER321_ID));
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_MSS_CELL_NAME.equals(rowData.getCell()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_CELL_30min_MsTerminating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(CELL_SQL_ID, Long.toString(TEST_VALUE_MSS_HIER321_ID));
        map.putSingle(EVENT_ID_PARAM, MSS_MS_TERMINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_TERMINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_MSS_CELL_NAME.equals(rowData.getCell()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_CELL_30min_LocationService() throws Exception {
        createLocServiceTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(CELL_SQL_ID, Long.toString(TEST_VALUE_MSS_HIER321_ID));
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(LOCATION_SERVICES.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_MSS_CELL_NAME.equals(rowData.getCell()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_CELL_30min_MsOriginatingSMS() throws Exception {
        createSmsTopologyTables(this.connection);
        createNPopulateErrTempTablesSms();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(CELL_SQL_ID, Long.toString(TEST_VALUE_MSS_HIER321_ID));
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING_SMS_MSC.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_MSS_CELL_NAME.equals(rowData.getCell()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_TAC_30min_MsOriginating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        populateGroupTacTable(this.connection);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, TEST_VALUE_TAC);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_TAC_30min_MsTerminating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        populateGroupTacTable(this.connection);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, TEST_VALUE_TAC);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_TERMINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_TERMINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_TAC_30min_RoamingCall() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        populateGroupTacTable(this.connection);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, TEST_VALUE_TAC);
        map.putSingle(EVENT_ID_PARAM, MSS_ROAMING_CALL_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(ROAMING_CALL_FORWARDING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_TAC_30min_LocationService() throws Exception {
        createLocServiceTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices();
        populateGroupTacTable(this.connection);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, TEST_VALUE_TAC);
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(LOCATION_SERVICES.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }

    }

    @Test
    public void testGetDetailedData_TAC_30min_MsOriginatingSMS() throws Exception {
        createSmsTopologyTables(this.connection);
        createNPopulateErrTempTablesSms();
        populateGroupTacTable(this.connection);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, TEST_VALUE_TAC);
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING_SMS_MSC.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }

    }

    @Test
    public void testGetDetailedData_EXCLUSIVE_TAC_GROUP_DRILL_30min_MsOriginating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        populateGroupTacTable(this.connection);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, TEST_VALUE_TAC);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }

    }

    @Test
    public void testGetDetailedData_EXCLUSIVE_TAC_GROUP_DRILL_30min_MsTerminating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        populateGroupTacTable(this.connection);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, TEST_VALUE_TAC);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_TERMINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_TERMINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }

    }

    @Test
    public void testGetDetailedData_EXCLUSIVE_TAC_GROUP_DRILL_30min_RoamingCall() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        populateGroupTacTable(this.connection);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, TEST_VALUE_TAC);
        map.putSingle(EVENT_ID_PARAM, MSS_ROAMING_CALL_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(ROAMING_CALL_FORWARDING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }

    }

    @Test
    public void testGetDetailedData_EXCLUSIVE_TAC_GROUP_DRILL_30min_LocationService() throws Exception {
        createLocServiceTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices();
        populateGroupTacTable(this.connection);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, TEST_VALUE_TAC);
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(LOCATION_SERVICES.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_EXCLUSIVE_TAC_GROUP_DRILL_30min_MsOriginatingSMS() throws Exception {
        createSmsTopologyTables(this.connection);
        createNPopulateErrTempTablesSms();
        populateGroupTacTable(this.connection);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, TEST_VALUE_TAC);
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING_SMS_MSC.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_MSCGroup_30min_MsOriginating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_EVNTSRC);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }

    }

    @Test
    public void testGetDetailedData_MSCGroup_30min_MsTerminating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_EVNTSRC);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_TERMINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_TERMINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_MSCGroup_30min_CallForwarding() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_EVNTSRC);
        map.putSingle(EVENT_ID_PARAM, MSS_CALL_FORWARDING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(CALL_FORWARDING.equals(rowData.getEventIdDesc()));
            assertTrue(rowData.getTac() == null || rowData.getTac().length() == 0);
            assertNull(rowData.getCell());
            assertNull(rowData.getController());
            assertNull(rowData.getRatDesc());
        }
    }

    @Test
    public void testGetDetailedData_MSCGroup_30min_RoamingCallForwarding() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_EVNTSRC);
        map.putSingle(EVENT_ID_PARAM, MSS_ROAMING_CALL_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(ROAMING_CALL_FORWARDING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
            assertNull(rowData.getCell());
            assertNull(rowData.getController());
            assertNull(rowData.getRatDesc());
        }
    }

    @Test
    public void testGetDetailedData_MSCGroup_30min_LocationService() throws Exception {
        createLocServiceTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_EVNTSRC);
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(LOCATION_SERVICES.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_MSCGroup_30min_MsOriginatingSMS() throws Exception {
        createSmsTopologyTables(this.connection);
        createNPopulateErrTempTablesSms();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_EVNTSRC);
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING_SMS_MSC.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_CONTROLLERGroup_30min_MsOriginating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CONTROLLER);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }

    }

    @Test
    public void testGetDetailedData_CONTROLLERGroup_30min_MsTerminating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CONTROLLER);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_TERMINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_TERMINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_CONTROLLERGroup_30min_LocationService() throws Exception {
        createLocServiceTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CONTROLLER);
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(LOCATION_SERVICES.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_CONTROLLERGroup_30min_MsOriginatingSMS() throws Exception {
        createSmsTopologyTables(this.connection);
        createNPopulateErrTempTablesSms();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CONTROLLER);
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING_SMS_MSC.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_CELLGroup_30min_MsOriginating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CELL);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }

    }

    @Test
    public void testGetDetailedData_CELLGroup_30min_MsTerminating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CELL);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_TERMINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_TERMINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_CELLGroup_30min_LocationService() throws Exception {
        createLocServiceTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CELL);
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(LOCATION_SERVICES.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_CELLGroup_30min_MsOriginatingSMS() throws Exception {
        createSmsTopologyTables(this.connection);
        createNPopulateErrTempTablesSms();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_CELL);
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING_SMS_MSC.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_TACGroup_30min_MsOriginating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_TAC_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_TACGroup_30min_MsTerminating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_TAC_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_TERMINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_TERMINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_TACGroup_30min_RoamingCall() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_TAC_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_ROAMING_CALL_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(ROAMING_CALL_FORWARDING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_TACGroup_30min_LocationService() throws Exception {
        createLocServiceTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_TAC_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(LOCATION_SERVICES.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_TACGroup_30min_MsOriginatingSMS() throws Exception {
        createSmsTopologyTables(this.connection);
        createNPopulateErrTempTablesSms();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_TAC_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING_SMS_MSC.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_ExclusiveTACGroup_30min_MsOriginating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, EXCLUSIVE_TAC_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_EXCLUSIVE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_ExclusiveTACGroup_30min_MsTerminating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, EXCLUSIVE_TAC_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_TERMINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_TERMINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_EXCLUSIVE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_ExclusiveTACGroup_30min_RoamingCall() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, EXCLUSIVE_TAC_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_ROAMING_CALL_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(ROAMING_CALL_FORWARDING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_EXCLUSIVE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_ExclusiveTACGroup_30min_LocationService() throws Exception {
        createLocServiceTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, EXCLUSIVE_TAC_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(LOCATION_SERVICES.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_EXCLUSIVE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_ExclusiveTACGroup_30min_MsOriginatingSMS() throws Exception {
        createSmsTopologyTables(this.connection);
        createNPopulateErrTempTablesSms();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, EXCLUSIVE_TAC_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING_SMS_MSC.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_EXCLUSIVE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_IMSIGroup_30min_MsOriginating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_IMSI_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }

    }

    @Test
    public void testGetDetailedData_IMSIGroup_30min_MsTerminating() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_IMSI_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_MS_TERMINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_TERMINATING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_IMSIGroup_30min_CallForwarding() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_IMSI_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_CALL_FORWARDING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(CALL_FORWARDING.equals(rowData.getEventIdDesc()));
            assertTrue(rowData.getTac() == null || rowData.getTac().length() == 0);
            assertNull(rowData.getCell());
            assertNull(rowData.getController());
            assertNull(rowData.getRatDesc());
        }
    }

    @Test
    public void testGetDetailedData_IMSIGroup_30min_RoamingCallForwarding() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_IMSI_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_ROAMING_CALL_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(2));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(ROAMING_CALL_FORWARDING.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
            assertNull(rowData.getCell());
            assertNull(rowData.getController());
            assertNull(rowData.getRatDesc());
        }
    }

    @Test
    public void testGetDetailedData_IMSIGroup_30min_LocationService() throws Exception {
        createLocServiceTopologyTables(this.connection);
        createNPopulateErrTempTablesLocServices();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_IMSI_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_LOCATION_SERVICE_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(LOCATION_SERVICES.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    @Test
    public void testGetDetailedData_IMSIGroup_30min_MsOriginatingSMS() throws Exception {
        createSmsTopologyTables(this.connection);
        createNPopulateErrTempTablesSms();
        createGroupTemporaryTopologyTables(this.connection);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_IMSI_GROUP);
        map.putSingle(EVENT_ID_PARAM, MSS_SMS_MS_ORIGINATING_EVENT_ID);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);
        assertThat(detailedResult.size(), is(1));
        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(MS_ORIGINATING_SMS_MSC.equals(rowData.getEventIdDesc()));
            assertTrue(TEST_VALUE_TAC.equals(rowData.getTac()));
        }
    }

    //------------------ Cause Code Analysis -----------------------------
    @Test
    public void testGetDetailedData_MSC_30min_All() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(EVENT_SOURCE_SQL_ID, Long.toString(TEST_VALUE_MSS_EVNTSRC_ID));

        map.putSingle(INTERNAL_CAUSE_CODE_PARAM, "48");
        map.putSingle(FAULT_CODE_PARAM, "467");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);

        assertThat(detailedResult.size(), is(8));

        for (final MssEventAnalysisDetailedResult rowData : detailedResult) {
            assertTrue(TEST_VALUE_MSS_EVNTSRC.equals(rowData.getEventSourceName()));
        }

    }

    @Test
    public void testGetDetailedData_MSCGroup_30min_All() throws Exception {
        createVoiceTempTopolgyTables(this.connection);
        createNPopulateErrTempTablesVoice();
        createGroupTemporaryTopologyTables(this.connection);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSS_GROUP_EVNTSRC);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(INTERNAL_CAUSE_CODE_PARAM, "48");
        map.putSingle(FAULT_CODE_PARAM, "467");
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfoMss(map, mssEventAnalysisResource);

        final String json = mssEventAnalysisResource.getData();
        final List<MssEventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json, MssEventAnalysisDetailedResult.class);

        assertThat(detailedResult.size(), is(8));
    }

    private void setupRawTableConstantValuesForMss(final String timestamp) {
        final String date_id = DateTimeUtilities.getTimeField(timestamp, "DATE_ID");
        final String hour_id = DateTimeUtilities.getTimeField(timestamp, "HOUR_ID");
        constantcolumnsInMssAllSchemas.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        constantcolumnsInMssAllSchemas.put(EVENT_TIME, timestamp);
        constantcolumnsInMssAllSchemas.put(DATETIME_ID, timestamp);
        constantcolumnsInMssAllSchemas.put(DATE_ID, date_id);
        constantcolumnsInMssAllSchemas.put(HOUR_ID, hour_id);
        constantcolumnsInMssAllSchemas.put(IMSI, TEST_VALUE_IMSI);
        constantcolumnsInMssAllSchemas.put(MSISDN, TEST_VALUE_MSISDN);
        constantcolumnsInMssAllSchemas.put(RECORD_SEQ_NUM, TEST_VALUE_RECORD_SEQ_NUM);
        constantcolumnsInMssAllSchemas.put(MCC, TEST_VALUE_MCC);
        constantcolumnsInMssAllSchemas.put(MNC, TEST_VALUE_MNC);
        constantcolumnsInMssAllSchemas.put(RAC, TEST_VALUE_RAC);
        constantcolumnsInMssAllSchemas.put(LAC, TEST_VALUE_LAC);
        constantcolumnsInMssAllSchemas.put(ROAMING, TEST_VALUE_ROAMING);
        constantcolumnsInMssAllSchemas.put(CALL_ID_NUM, TEST_VALUE_CALL_ID_NUM);
        constantcolumnsInMssAllSchemas.put(HIER3_ID, TEST_VALUE_MSS_HIER3_ID);
        constantcolumnsInMssAllSchemas.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        constantcolumnsInMssAllSchemas.put(RAT, RAT_FOR_GSM);
        constantcolumnsInMssAllSchemas.put(IMSI_MCC, TEST_VALUE_IMSI_MCC);
        constantcolumnsInMssAllSchemas.put(IMSI_MNC, TEST_VALUE_IMSI_MNC);
        final String date_id_week = DateTimeUtilities.getTimeField(DateTimeUtilities.getDateTimeMinus36Hours(), "DATE_ID");
        constantcolumnsInMssAllSchemas.put(LOCAL_DATE_ID, date_id_week);
    }

    private void setupRawTableConstantValuesForMssDay(final String timestamp) {
        constantcolumnsInMssAllSchemasDay.put(EVNTSRC_ID, TEST_VALUE_MSS_EVNTSRC_ID);
        constantcolumnsInMssAllSchemasDay.put(DATETIME_ID, timestamp);
    }

    private void setupRawTableConstantValuesForVoice() {
        constantcolumnsInMssVoiceRawTables.put(INTERNAL_CAUSE_CODE, TEST_VALUE_INTERNAL_CAUSE_CODE);
        constantcolumnsInMssVoiceRawTables.put(EXTERNAL_CAUSE_CODE, TEST_VALUE_EXTERNAL_CAUSE_CODE);
        constantcolumnsInMssVoiceRawTables.put(EXTERNAL_PROTOCOL_ID, TEST_VALUE_EXTERNAL_PROTOCOL_ID);
        constantcolumnsInMssVoiceRawTables.put(FAULT_CODE, TEST_VALUE_FAULT_CODE);
        constantcolumnsInMssVoiceRawTables.put(INCOMING_ROUTE, TEST_VALUE_INCOMING_ROUTE);
        constantcolumnsInMssVoiceRawTables.put(OUTGOING_ROUTE, TEST_VALUE_OUTGOING_ROUTE);
        constantcolumnsInMssVoiceRawTables.put(INTERNAL_LOCATION_CODE, TEST_VALUE_INTERNAL_LOCATION_CODE);
        constantcolumnsInMssVoiceRawTables.put(BEARER_SERVICE_CODE, TEST_VALUE_BEARER_SERVICE_CODE);
        constantcolumnsInMssVoiceRawTables.put(TELE_SERVICE_CODE, TEST_VALUE_TELE_SERVICE_CODE);
        constantcolumnsInMssVoiceRawTables.put(TYPE_OF_CALLING_SUB, TEST_VALUE_TYPE_OF_CALLING_SUB);
        constantcolumnsInMssVoiceRawTables.put(CALLING_PARTY_NUM, TEST_VALUE_CALLING_PARTY_NUM);
        constantcolumnsInMssVoiceRawTables.put(CALLED_PARTY_NUM, TEST_VALUE_CALLED_PARTY_NUM);
        constantcolumnsInMssVoiceRawTables.put(CALLING_SUB_IMSI, TEST_VALUE_CALLING_SUB_IMSI);
        constantcolumnsInMssVoiceRawTables.put(CALLED_SUB_IMSI, TEST_VALUE_CALLED_SUB_IMSI);
        constantcolumnsInMssVoiceRawTables.put(CALLING_SUB_IMEI, TEST_VALUE_CALLING_SUB_IMEI);
        constantcolumnsInMssVoiceRawTables.put(CALLED_SUB_IMEI, TEST_VALUE_CALLED_SUB_IMEI);
        constantcolumnsInMssVoiceRawTables.put(MS_ROAMING_NUM, TEST_VALUE_MS_ROAMING_NUM);
        constantcolumnsInMssVoiceRawTables.put(DISCONNECT_PARTY, TEST_VALUE_DISCONNECT_PARTY);
        constantcolumnsInMssVoiceRawTables.put(CALL_DURATION, TEST_VALUE_CALL_DURATION);
        constantcolumnsInMssVoiceRawTables.put(SEIZURE_TIME, TEST_VALUE_SEIZURE_TIME);
        constantcolumnsInMssVoiceRawTables.put(ORIGINAL_CALLED_NUM, TEST_VALUE_ORIGINAL_CALLED_NUM);
        constantcolumnsInMssVoiceRawTables.put(REDIRECT_NUM, TEST_VALUE_REDIRECT_NUM);
        constantcolumnsInMssVoiceRawTables.put(REDIRECT_COUNTER, TEST_VALUE_REDIRECT_COUNTER);
        constantcolumnsInMssVoiceRawTables.put(REDIRECT_IMSI, TEST_VALUE_REDIRECT_IMSI);
        constantcolumnsInMssVoiceRawTables.put(REDIRECT_SPN, TEST_VALUE_REDIRECT_SPN);
        constantcolumnsInMssVoiceRawTables.put(CALL_POSITION, TEST_VALUE_CALL_POSITION);
        constantcolumnsInMssVoiceRawTables.put(EOS_INFO, TEST_VALUE_EOS_INFO);
        constantcolumnsInMssVoiceRawTables.put(NETWORK_CALL_REFERENCE, TEST_VALUE_NETWORK_CALL_REFERENCE);
    }

    private void setupRawTableConstantValuesForLocationService() {
        constantcolumnsInMssLocationRawTables.put(LCS_CLIENT_TYPE, TEST_VALUE_LCS_CLIENT_TYPE);
        constantcolumnsInMssLocationRawTables.put(UNSUC_POSITION_REASON, TEST_VALUE_UNSUC_POSITION_REASON);
        constantcolumnsInMssLocationRawTables.put(TYPE_LOCATION_REQ, TEST_VALUE_TYPE_LOCATION_REQ);
        constantcolumnsInMssLocationRawTables.put(TARGET_MSISDN, TEST_VALUE_TARGET_MSISDN);
        constantcolumnsInMssLocationRawTables.put(TARGET_IMSI, TEST_VALUE_TARGET_IMSI);
        constantcolumnsInMssLocationRawTables.put(TARGET_IMEI, TEST_VALUE_TARGET_IMEI);
        constantcolumnsInMssLocationRawTables.put(LCS_CLIENT_ID, TEST_VALUE_LCS_CLIENT_ID);
        constantcolumnsInMssLocationRawTables.put(POSITION_DELIVERY, TEST_VALUE_POSITION_DELIVERY);
        constantcolumnsInMssLocationRawTables.put(NETWORK_CALL_REFERENCE, TEST_VALUE_NETWORK_CALL_REFERENCE);
    }

    private void setupRawTableConstantValuesForSms() {
        constantcolumnsInMssSmsRawTables.put(SMS_RESULT, TEST_VALUE_SMS_RESULT);
        constantcolumnsInMssSmsRawTables.put(MSG_TYPE_INDICATOR, TEST_VALUE_MSG_TYPE_INDICATOR);
        constantcolumnsInMssSmsRawTables.put(CALLING_SUB_IMEISV, TEST_VALUE_CALLING_SUB_IMEISV);
        constantcolumnsInMssSmsRawTables.put(CALLED_SUB_IMEISV, TEST_VALUE_CALLED_SUB_IMEISV);
        constantcolumnsInMssSmsRawTables.put(ORIGINATING_NUM, TEST_VALUE_ORIGINATING_NUM);
        constantcolumnsInMssSmsRawTables.put(DEST_NUM, TEST_VALUE_DEST_NUM);
        constantcolumnsInMssSmsRawTables.put(SERVICE_CENTRE, TEST_VALUE_SERVICE_CENTRE);
        constantcolumnsInMssSmsRawTables.put(ORIGINATING_TIME, TEST_VALUE_ORIGINATING_TIME);
        constantcolumnsInMssSmsRawTables.put(DELIVERY_TIME, TEST_VALUE_DELIVERY_TIME);
        constantcolumnsInMssSmsRawTables.put(BEARER_SERVICE_CODE, TEST_VALUE_BEARER_SERVICE_CODE);
        constantcolumnsInMssSmsRawTables.put(TELE_SERVICE_CODE, TEST_VALUE_TELE_SERVICE_CODE);
        constantcolumnsInMssSmsRawTables.put(TYPE_OF_CALLING_SUB, TEST_VALUE_TYPE_OF_CALLING_SUB);
        constantcolumnsInMssSmsRawTables.put(CALLING_PARTY_NUM, TEST_VALUE_CALLING_PARTY_NUM);
        constantcolumnsInMssSmsRawTables.put(CALLED_PARTY_NUM, TEST_VALUE_CALLED_PARTY_NUM);
        constantcolumnsInMssSmsRawTables.put(CALLING_SUB_IMSI, TEST_VALUE_CALLING_SUB_IMSI);
        constantcolumnsInMssSmsRawTables.put(CALLED_SUB_IMSI, TEST_VALUE_CALLED_SUB_IMSI);
        constantcolumnsInMssSmsRawTables.put(CALLING_SUB_IMEI, TEST_VALUE_CALLING_SUB_IMEI);
        constantcolumnsInMssSmsRawTables.put(CALLED_SUB_IMEI, TEST_VALUE_CALLED_SUB_IMEI);
    }

    private void setupRawTableValues(final Map<String, Object> mapToPopulate, final String tac, final int eventId) {
        mapToPopulate.put(EVENT_ID, eventId);
        mapToPopulate.put(TAC, tac);
    }

    private void setupDayTableValues(final Map<String, Object> mapToPopulate, final int no_of_error, final int no_of_success, final long evntsrcId,
                                     final int eventId) {
        mapToPopulate.put(NO_OF_ERRORS, no_of_error);
        mapToPopulate.put(NO_OF_SUCCESSES, no_of_success);
        mapToPopulate.put(EVNTSRC_ID, evntsrcId);
        mapToPopulate.put(EVENT_ID, eventId);
    }

    private void setUpControllerCellNullValues(final Map<String, Object> mapToPopulate) {
        mapToPopulate.put(HIER3_ID, NULL_VALUE);
        mapToPopulate.put(HIER321_ID, NULL_VALUE);
        mapToPopulate.put(RAT, NULL_VALUE);
        mapToPopulate.put(IMSI_MCC, NULL_VALUE);
        mapToPopulate.put(IMSI_MNC, NULL_VALUE);
    }

    private void setUpControllerCellValidValues(final Map<String, Object> mapToPopulate) {
        mapToPopulate.put(HIER3_ID, TEST_VALUE_MSS_HIER3_ID);
        mapToPopulate.put(HIER321_ID, TEST_VALUE_MSS_HIER321_ID);
        mapToPopulate.put(RAT, RAT_FOR_GSM);
        mapToPopulate.put(IMSI_MCC, TEST_VALUE_IMSI_MCC);
        mapToPopulate.put(IMSI_MNC, TEST_VALUE_IMSI_MNC);
    }

    private void createNPopulateErrTempTablesVoice() throws Exception {
        createNPopulateErrTempTablesVoice(this.connection);
    }

    private void createNPopulateErrTempTablesVoice(final Connection connection1) throws Exception {
        final Collection<String> columnsToCreateInVoiceTables = new ArrayList<String>();
        columnsToCreateInVoiceTables.addAll(constantcolumnsInMssAllSchemas.keySet());
        columnsToCreateInVoiceTables.addAll(constantcolumnsInMssVoiceRawTables.keySet());
        columnsToCreateInVoiceTables.add(TAC);
        columnsToCreateInVoiceTables.add(EVENT_ID);
        for (final String tempTable : tempErrRawTablesVoice) {
            createTemporaryTableOnSpecificConnectionMss(connection1, tempTable, columnsToCreateInVoiceTables);
        }
        populateErrVoiceTables(connection1);
    }

    private void createNPopulateErrTempTablesVoiceWeek(final Connection connection1) throws Exception {
        final Collection<String> columnsToCreateInVoiceTablesDay = new ArrayList<String>();
        columnsToCreateInVoiceTablesDay.addAll(constantcolumnsInMssAllSchemasDay.keySet());
        columnsToCreateInVoiceTablesDay.addAll(constantcolumnsInMssVoiceDayTables.keySet());
        columnsToCreateInVoiceTablesDay.add(EVENT_ID);
        columnsToCreateInVoiceTablesDay.add(NO_OF_ERRORS);
        columnsToCreateInVoiceTablesDay.add(NO_OF_SUCCESSES);
        columnsToCreateInVoiceTablesDay.add(EVNTSRC_ID);
        for (final String tempTable : tempErrDayTablesVoice) {
            createTemporaryTableOnSpecificConnectionMss(connection1, tempTable, columnsToCreateInVoiceTablesDay);
        }

        populateErrVoiceTablesWeek(connection1);
    }

    private void createNPopulateSucTempTablesVoice() throws Exception {
        createNPopulateSucTempTablesVoice(this.connection);
    }

    private void createNPopulateSucTempTablesVoice(final Connection connection1) throws Exception {
        final Collection<String> columnsToCreateInVoiceTables = new ArrayList<String>();
        columnsToCreateInVoiceTables.addAll(constantcolumnsInMssAllSchemas.keySet());
        columnsToCreateInVoiceTables.addAll(constantcolumnsInMssVoiceRawTables.keySet());
        columnsToCreateInVoiceTables.add(TAC);
        columnsToCreateInVoiceTables.add(EVENT_ID);
        createTemporaryTableOnSpecificConnectionMss(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW, columnsToCreateInVoiceTables);
        populateSucVoiceTables(connection1);
    }

    private void createNPopulateSucTempTablesVoiceWeek(final Connection connection1) throws Exception {
        final Collection<String> columnsToCreateInVoiceTablesDay = new ArrayList<String>();
        columnsToCreateInVoiceTablesDay.addAll(constantcolumnsInMssAllSchemasDay.keySet());
        columnsToCreateInVoiceTablesDay.addAll(constantcolumnsInMssVoiceDayTables.keySet());
        columnsToCreateInVoiceTablesDay.add(EVENT_ID);
        columnsToCreateInVoiceTablesDay.add(NO_OF_ERRORS);
        columnsToCreateInVoiceTablesDay.add(NO_OF_SUCCESSES);
        columnsToCreateInVoiceTablesDay.add(EVNTSRC_ID);

        createTemporaryTableOnSpecificConnectionMss(connection1, TEMP_VOICE_EVENT_SRC_SUM_SUC_DAY, columnsToCreateInVoiceTablesDay);
        populateSucVoiceTablesWeek(connection1);
    }

    private void createNPopulateErrTempTablesLocServices() throws Exception {
        createNPopulateErrTempTablesLocServices(this.connection);
    }

    private void createNPopulateErrTempTablesLocServices(final Connection connection1) throws Exception {
        final Collection<String> columnsToCreateInLocServiceTables = new ArrayList<String>();
        columnsToCreateInLocServiceTables.addAll(constantcolumnsInMssAllSchemas.keySet());
        columnsToCreateInLocServiceTables.addAll(constantcolumnsInMssLocationRawTables.keySet());
        columnsToCreateInLocServiceTables.add(TAC);
        columnsToCreateInLocServiceTables.add(EVENT_ID);
        for (final String tempTable : tempErrRawTablesLocationService) {
            createTemporaryTableOnSpecificConnectionMss(connection1, tempTable, columnsToCreateInLocServiceTables);
        }
        populateLocationServiceTables(connection1);
    }

    private void createNPopulateErrTempTablesLocServicesWeek(final Connection connection1) throws Exception {
        final Collection<String> columnsToCreateInLocServiceTables = new ArrayList<String>();
        columnsToCreateInLocServiceTables.addAll(constantcolumnsInMssAllSchemasDay.keySet());
        columnsToCreateInLocServiceTables.addAll(constantcolumnsInMssLocationDayTables.keySet());
        columnsToCreateInLocServiceTables.add(EVENT_ID);
        columnsToCreateInLocServiceTables.add(NO_OF_ERRORS);
        columnsToCreateInLocServiceTables.add(NO_OF_SUCCESSES);
        columnsToCreateInLocServiceTables.add(EVNTSRC_ID);
        for (final String tempTable : tempErrDayTablesLocService) {
            createTemporaryTableOnSpecificConnectionMss(connection1, tempTable, columnsToCreateInLocServiceTables);
        }

        populateErrLocationServiceTablesWeek(connection1);
    }

    private void createNPopulateSucTempTablesLocServices(final Connection connection1) throws Exception {
        final Collection<String> columnsToCreateInLocServiceTables = new ArrayList<String>();
        columnsToCreateInLocServiceTables.addAll(constantcolumnsInMssAllSchemas.keySet());
        columnsToCreateInLocServiceTables.addAll(constantcolumnsInMssLocationRawTables.keySet());
        columnsToCreateInLocServiceTables.add(TAC);
        columnsToCreateInLocServiceTables.add(EVENT_ID);
        createTemporaryTableOnSpecificConnectionMss(connection1, TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_SUC_RAW, columnsToCreateInLocServiceTables);

        populateSucLocationServiceTables(connection1);
    }

    private void createNPopulateSucTempTablesLocServicesWeek(final Connection connection1) throws Exception {
        final Collection<String> columnsToCreateInLocServiceTables = new ArrayList<String>();
        columnsToCreateInLocServiceTables.addAll(constantcolumnsInMssAllSchemasDay.keySet());
        columnsToCreateInLocServiceTables.addAll(constantcolumnsInMssLocationDayTables.keySet());
        columnsToCreateInLocServiceTables.add(EVENT_ID);
        columnsToCreateInLocServiceTables.add(NO_OF_ERRORS);
        columnsToCreateInLocServiceTables.add(NO_OF_SUCCESSES);
        columnsToCreateInLocServiceTables.add(EVNTSRC_ID);

        createTemporaryTableOnSpecificConnectionMss(connection1, TEMP_LOC_EVENT_SRC_SUM_SUC_DAY, columnsToCreateInLocServiceTables);
        populateSucLocationServiceTablesWeek(connection1);
    }

    private void createNPopulateErrTempTablesSms() throws Exception {
        createNPopulateErrTempTablesSms(this.connection);
    }

    private void createNPopulateErrTempTablesSms(final Connection connection1) throws Exception {
        final Collection<String> columnsToCreateInSmsTables = new ArrayList<String>();
        columnsToCreateInSmsTables.addAll(constantcolumnsInMssAllSchemas.keySet());
        columnsToCreateInSmsTables.addAll(constantcolumnsInMssSmsRawTables.keySet());
        columnsToCreateInSmsTables.add(TAC);
        columnsToCreateInSmsTables.add(EVENT_ID);
        for (final String tempTable : tempErrRawTablesSms) {
            createTemporaryTableOnSpecificConnectionMss(connection1, tempTable, columnsToCreateInSmsTables);
        }
        populateErrSmsTables(connection1);
    }

    private void createNPopulateErrTempTablesSmsWeek(final Connection connection1) throws Exception {
        final Collection<String> columnsToCreateInSmsTables = new ArrayList<String>();
        columnsToCreateInSmsTables.addAll(constantcolumnsInMssAllSchemasDay.keySet());
        columnsToCreateInSmsTables.addAll(constantcolumnsInMssSmsDayTables.keySet());
        columnsToCreateInSmsTables.add(EVENT_ID);
        columnsToCreateInSmsTables.add(NO_OF_ERRORS);
        columnsToCreateInSmsTables.add(NO_OF_SUCCESSES);
        columnsToCreateInSmsTables.add(EVNTSRC_ID);
        for (final String tempTable : tempErrDayTablesSms) {
            createTemporaryTableOnSpecificConnectionMss(connection1, tempTable, columnsToCreateInSmsTables);
        }

        populateErrSmsTablesWeek(connection1);
    }

    private void createNPopulateSucTempTablesSms(final Connection connection1) throws Exception {
        final Collection<String> columnsToCreateInVoiceTables = new ArrayList<String>();
        columnsToCreateInVoiceTables.addAll(constantcolumnsInMssAllSchemas.keySet());
        columnsToCreateInVoiceTables.addAll(constantcolumnsInMssSmsRawTables.keySet());
        columnsToCreateInVoiceTables.add(TAC);
        columnsToCreateInVoiceTables.add(EVENT_ID);
        createTemporaryTableOnSpecificConnectionMss(connection1, TEMP_EVENT_E_MSS_SMS_CDR_SUC_RAW, columnsToCreateInVoiceTables);
        populateSucSmsTables(connection1);
    }

    private void createNPopulateSucTempTablesSmsWeek(final Connection connection1) throws Exception {
        final Collection<String> columnsToCreateInSmsTables = new ArrayList<String>();
        columnsToCreateInSmsTables.addAll(constantcolumnsInMssAllSchemasDay.keySet());
        columnsToCreateInSmsTables.addAll(constantcolumnsInMssSmsDayTables.keySet());
        columnsToCreateInSmsTables.add(EVENT_ID);
        columnsToCreateInSmsTables.add(NO_OF_ERRORS);
        columnsToCreateInSmsTables.add(NO_OF_SUCCESSES);
        columnsToCreateInSmsTables.add(EVNTSRC_ID);

        createTemporaryTableOnSpecificConnectionMss(connection1, TEMP_SMS_EVNTSRC_EVENTID_SUC_DAY, columnsToCreateInSmsTables);
        populateSucSmsTablesWeek(connection1);
    }

    private void populateErrVoiceTables(final Connection connection1) throws Exception {
        final Map<String, Object> valuesForVoiceRawTables = new HashMap<String, Object>();
        valuesForVoiceRawTables.putAll(constantcolumnsInMssAllSchemas);
        valuesForVoiceRawTables.putAll(constantcolumnsInMssVoiceRawTables);

        setupRawTableValues(valuesForVoiceRawTables, TEST_VALUE_TAC, 0);
        setUpControllerCellValidValues(valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, valuesForVoiceRawTables);

        setupRawTableValues(valuesForVoiceRawTables, TEST_VALUE_TAC, 1);
        setUpControllerCellValidValues(valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, valuesForVoiceRawTables);

        setupRawTableValues(valuesForVoiceRawTables, NULL_VALUE, 2);
        setUpControllerCellNullValues(valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, valuesForVoiceRawTables);

        setupRawTableValues(valuesForVoiceRawTables, TEST_VALUE_TAC, 3);
        setUpControllerCellNullValues(valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, valuesForVoiceRawTables);

        setupRawTableValues(valuesForVoiceRawTables, TEST_VALUE_EXCLUSIVE_TAC, 0);
        setUpControllerCellValidValues(valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, valuesForVoiceRawTables);

        setupRawTableValues(valuesForVoiceRawTables, TEST_VALUE_EXCLUSIVE_TAC, 1);
        setUpControllerCellValidValues(valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, valuesForVoiceRawTables);

        setupRawTableValues(valuesForVoiceRawTables, TEST_VALUE_EXCLUSIVE_TAC, 3);
        setUpControllerCellNullValues(valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_ERR_RAW, valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW, valuesForVoiceRawTables);
    }

    private void populateErrVoiceTablesWeek(final Connection connection1) throws Exception {
        final Map<String, Object> valuesForVoiceDayTables = new HashMap<String, Object>();

        valuesForVoiceDayTables.putAll(constantcolumnsInMssAllSchemasDay);
        valuesForVoiceDayTables.putAll(constantcolumnsInMssVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 1);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_ERR_DAY, valuesForVoiceDayTables);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_DROP_DAY, valuesForVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 2);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_ERR_DAY, valuesForVoiceDayTables);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_DROP_DAY, valuesForVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 3);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_ERR_DAY, valuesForVoiceDayTables);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_DROP_DAY, valuesForVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 0);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_ERR_DAY, valuesForVoiceDayTables);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_DROP_DAY, valuesForVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 1);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_ERR_DAY, valuesForVoiceDayTables);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_DROP_DAY, valuesForVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 2);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_ERR_DAY, valuesForVoiceDayTables);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_DROP_DAY, valuesForVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 3);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_ERR_DAY, valuesForVoiceDayTables);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_DROP_DAY, valuesForVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 0);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_ERR_DAY, valuesForVoiceDayTables);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_DROP_DAY, valuesForVoiceDayTables);
    }

    private void populateSucVoiceTables(final Connection connection1) throws Exception {
        final Map<String, Object> valuesForVoiceRawTables = new HashMap<String, Object>();
        valuesForVoiceRawTables.putAll(constantcolumnsInMssAllSchemas);
        valuesForVoiceRawTables.putAll(constantcolumnsInMssVoiceRawTables);

        setupRawTableValues(valuesForVoiceRawTables, TEST_VALUE_TAC, 0);
        setUpControllerCellValidValues(valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW, valuesForVoiceRawTables);

        setupRawTableValues(valuesForVoiceRawTables, TEST_VALUE_TAC, 1);
        setUpControllerCellValidValues(valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW, valuesForVoiceRawTables);

        setupRawTableValues(valuesForVoiceRawTables, NULL_VALUE, 2);
        setUpControllerCellNullValues(valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW, valuesForVoiceRawTables);

        setupRawTableValues(valuesForVoiceRawTables, TEST_VALUE_TAC, 3);
        setUpControllerCellNullValues(valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW, valuesForVoiceRawTables);

        setupRawTableValues(valuesForVoiceRawTables, TEST_VALUE_EXCLUSIVE_TAC, 0);
        setUpControllerCellValidValues(valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW, valuesForVoiceRawTables);

        setupRawTableValues(valuesForVoiceRawTables, TEST_VALUE_EXCLUSIVE_TAC, 1);
        setUpControllerCellValidValues(valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW, valuesForVoiceRawTables);

        setupRawTableValues(valuesForVoiceRawTables, TEST_VALUE_EXCLUSIVE_TAC, 3);
        setUpControllerCellNullValues(valuesForVoiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_VOICE_CDR_SUC_RAW, valuesForVoiceRawTables);
    }

    private void populateSucVoiceTablesWeek(final Connection connection1) throws Exception {
        final Map<String, Object> valuesForVoiceDayTables = new HashMap<String, Object>();

        valuesForVoiceDayTables.putAll(constantcolumnsInMssAllSchemasDay);
        valuesForVoiceDayTables.putAll(constantcolumnsInMssVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, 0, TEST_VALUE_NO_OF_SUCCESSES, TEST_VALUE_MSS_EVNTSRC_ID, 1);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_SUC_DAY, valuesForVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, 0, TEST_VALUE_NO_OF_SUCCESSES, TEST_VALUE_MSS_EVNTSRC_ID, 2);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_SUC_DAY, valuesForVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, 0, TEST_VALUE_NO_OF_SUCCESSES, TEST_VALUE_MSS_EVNTSRC_ID, 3);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_SUC_DAY, valuesForVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, 0, TEST_VALUE_NO_OF_SUCCESSES, TEST_VALUE_MSS_EVNTSRC_ID, 0);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_SUC_DAY, valuesForVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, 0, TEST_VALUE_NO_OF_SUCCESSES, TEST_VALUE_MSS_EVNTSRC_ID, 0);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_SUC_DAY, valuesForVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, 0, TEST_VALUE_NO_OF_SUCCESSES, TEST_VALUE_MSS_EVNTSRC_ID, 1);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_SUC_DAY, valuesForVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, 0, TEST_VALUE_NO_OF_SUCCESSES, TEST_VALUE_MSS_EVNTSRC_ID, 2);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_SUC_DAY, valuesForVoiceDayTables);

        setupDayTableValues(valuesForVoiceDayTables, 0, TEST_VALUE_NO_OF_SUCCESSES, TEST_VALUE_MSS_EVNTSRC_ID, 3);
        insertRowOnSpecificConnection(connection1, TEMP_VOICE_EVENT_SRC_SUM_SUC_DAY, valuesForVoiceDayTables);
    }

    private void populateLocationServiceTables(final Connection connection1) throws Exception {
        final Map<String, Object> valuesForLocServiceRawTables = new HashMap<String, Object>();
        valuesForLocServiceRawTables.putAll(constantcolumnsInMssAllSchemas);
        valuesForLocServiceRawTables.putAll(constantcolumnsInMssLocationRawTables);

        setupRawTableValues(valuesForLocServiceRawTables, TEST_VALUE_TAC, 6);
        setUpControllerCellValidValues(valuesForLocServiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_ERR_RAW, valuesForLocServiceRawTables);

        setupRawTableValues(valuesForLocServiceRawTables, TEST_VALUE_EXCLUSIVE_TAC, 6);
        setUpControllerCellValidValues(valuesForLocServiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_ERR_RAW, valuesForLocServiceRawTables);
    }

    private void populateErrLocationServiceTablesWeek(final Connection connection1) throws Exception {
        final Map<String, Object> valuesForLocDayTables = new HashMap<String, Object>();

        valuesForLocDayTables.putAll(constantcolumnsInMssAllSchemasDay);
        valuesForLocDayTables.putAll(constantcolumnsInMssVoiceDayTables);

        setupDayTableValues(valuesForLocDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 6);
        insertRowOnSpecificConnection(connection1, TEMP_LOC_EVENT_SRC_SUM_ERR_DAY, valuesForLocDayTables);

        setupDayTableValues(valuesForLocDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 6);
        insertRowOnSpecificConnection(connection1, TEMP_LOC_EVENT_SRC_SUM_ERR_DAY, valuesForLocDayTables);

        setupDayTableValues(valuesForLocDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 6);
        insertRowOnSpecificConnection(connection1, TEMP_LOC_EVENT_SRC_SUM_ERR_DAY, valuesForLocDayTables);

        setupDayTableValues(valuesForLocDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 6);
        insertRowOnSpecificConnection(connection1, TEMP_LOC_EVENT_SRC_SUM_ERR_DAY, valuesForLocDayTables);

    }

    private void populateSucLocationServiceTables(final Connection connection1) throws Exception {
        final Map<String, Object> valuesForLocServiceRawTables = new HashMap<String, Object>();
        valuesForLocServiceRawTables.putAll(constantcolumnsInMssAllSchemas);
        valuesForLocServiceRawTables.putAll(constantcolumnsInMssLocationRawTables);

        setupRawTableValues(valuesForLocServiceRawTables, TEST_VALUE_TAC, 6);
        setUpControllerCellValidValues(valuesForLocServiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_SUC_RAW, valuesForLocServiceRawTables);

        setupRawTableValues(valuesForLocServiceRawTables, TEST_VALUE_EXCLUSIVE_TAC, 6);
        setUpControllerCellValidValues(valuesForLocServiceRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_LOC_SERVICE_CDR_SUC_RAW, valuesForLocServiceRawTables);
    }

    private void populateSucLocationServiceTablesWeek(final Connection connection1) throws Exception {
        final Map<String, Object> valuesForLocDayTables = new HashMap<String, Object>();

        valuesForLocDayTables.putAll(constantcolumnsInMssAllSchemasDay);
        valuesForLocDayTables.putAll(constantcolumnsInMssVoiceDayTables);

        setupDayTableValues(valuesForLocDayTables, 0, TEST_VALUE_NO_OF_SUCCESSES, TEST_VALUE_MSS_EVNTSRC_ID, 6);
        insertRowOnSpecificConnection(connection1, TEMP_LOC_EVENT_SRC_SUM_SUC_DAY, valuesForLocDayTables);

        setupDayTableValues(valuesForLocDayTables, 0, TEST_VALUE_NO_OF_SUCCESSES, TEST_VALUE_MSS_EVNTSRC_ID, 6);
        insertRowOnSpecificConnection(connection1, TEMP_LOC_EVENT_SRC_SUM_SUC_DAY, valuesForLocDayTables);

    }

    private void populateErrSmsTables(final Connection connection1) throws Exception {
        final Map<String, Object> valuesForSmsRawTables = new HashMap<String, Object>();
        valuesForSmsRawTables.putAll(constantcolumnsInMssAllSchemas);
        valuesForSmsRawTables.putAll(constantcolumnsInMssSmsRawTables);

        setupRawTableValues(valuesForSmsRawTables, TEST_VALUE_TAC, 4);
        setUpControllerCellValidValues(valuesForSmsRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_SMS_CDR_ERR_RAW, valuesForSmsRawTables);

        setupRawTableValues(valuesForSmsRawTables, TEST_VALUE_TAC, 5);
        setUpControllerCellValidValues(valuesForSmsRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_SMS_CDR_ERR_RAW, valuesForSmsRawTables);

        setupRawTableValues(valuesForSmsRawTables, TEST_VALUE_EXCLUSIVE_TAC, 4);
        setUpControllerCellValidValues(valuesForSmsRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_SMS_CDR_ERR_RAW, valuesForSmsRawTables);

        setupRawTableValues(valuesForSmsRawTables, TEST_VALUE_EXCLUSIVE_TAC, 5);
        setUpControllerCellValidValues(valuesForSmsRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_SMS_CDR_ERR_RAW, valuesForSmsRawTables);
    }

    private void populateErrSmsTablesWeek(final Connection connection1) throws Exception {
        final Map<String, Object> valuesForSmsDayTables = new HashMap<String, Object>();

        valuesForSmsDayTables.putAll(constantcolumnsInMssAllSchemasDay);
        valuesForSmsDayTables.putAll(constantcolumnsInMssSmsDayTables);

        setupDayTableValues(valuesForSmsDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 4);
        insertRowOnSpecificConnection(connection1, TEMP_SMS_EVNTSRC_EVENTID_ERR_DAY, valuesForSmsDayTables);

        setupDayTableValues(valuesForSmsDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 5);
        insertRowOnSpecificConnection(connection1, TEMP_SMS_EVNTSRC_EVENTID_ERR_DAY, valuesForSmsDayTables);

        setupDayTableValues(valuesForSmsDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 4);
        insertRowOnSpecificConnection(connection1, TEMP_SMS_EVNTSRC_EVENTID_ERR_DAY, valuesForSmsDayTables);

        setupDayTableValues(valuesForSmsDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 5);
        insertRowOnSpecificConnection(connection1, TEMP_SMS_EVNTSRC_EVENTID_ERR_DAY, valuesForSmsDayTables);

        setupDayTableValues(valuesForSmsDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 4);
        insertRowOnSpecificConnection(connection1, TEMP_SMS_EVNTSRC_EVENTID_ERR_DAY, valuesForSmsDayTables);

        setupDayTableValues(valuesForSmsDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 5);
        insertRowOnSpecificConnection(connection1, TEMP_SMS_EVNTSRC_EVENTID_ERR_DAY, valuesForSmsDayTables);

        setupDayTableValues(valuesForSmsDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 4);
        insertRowOnSpecificConnection(connection1, TEMP_SMS_EVNTSRC_EVENTID_ERR_DAY, valuesForSmsDayTables);

        setupDayTableValues(valuesForSmsDayTables, TEST_VALUE_NO_OF_ERRORS, 0, TEST_VALUE_MSS_EVNTSRC_ID, 5);
        insertRowOnSpecificConnection(connection1, TEMP_SMS_EVNTSRC_EVENTID_ERR_DAY, valuesForSmsDayTables);

    }

    private void populateSucSmsTables(final Connection connection1) throws Exception {
        final Map<String, Object> valuesForSmsRawTables = new HashMap<String, Object>();
        valuesForSmsRawTables.putAll(constantcolumnsInMssAllSchemas);
        valuesForSmsRawTables.putAll(constantcolumnsInMssSmsRawTables);

        setupRawTableValues(valuesForSmsRawTables, TEST_VALUE_TAC, 4);
        setUpControllerCellValidValues(valuesForSmsRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_SMS_CDR_SUC_RAW, valuesForSmsRawTables);

        setupRawTableValues(valuesForSmsRawTables, TEST_VALUE_TAC, 5);
        setUpControllerCellValidValues(valuesForSmsRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_SMS_CDR_SUC_RAW, valuesForSmsRawTables);

        setupRawTableValues(valuesForSmsRawTables, TEST_VALUE_EXCLUSIVE_TAC, 4);
        setUpControllerCellValidValues(valuesForSmsRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_SMS_CDR_SUC_RAW, valuesForSmsRawTables);

        setupRawTableValues(valuesForSmsRawTables, TEST_VALUE_EXCLUSIVE_TAC, 5);
        setUpControllerCellValidValues(valuesForSmsRawTables);
        insertRowOnSpecificConnection(connection1, TEMP_EVENT_E_MSS_SMS_CDR_SUC_RAW, valuesForSmsRawTables);
    }

    private void populateSucSmsTablesWeek(final Connection connection1) throws Exception {
        final Map<String, Object> valuesForSmsDayTables = new HashMap<String, Object>();

        valuesForSmsDayTables.putAll(constantcolumnsInMssAllSchemasDay);
        valuesForSmsDayTables.putAll(constantcolumnsInMssSmsDayTables);

        setupDayTableValues(valuesForSmsDayTables, 0, TEST_VALUE_NO_OF_SUCCESSES, TEST_VALUE_MSS_EVNTSRC_ID, 4);
        insertRowOnSpecificConnection(connection1, TEMP_SMS_EVNTSRC_EVENTID_SUC_DAY, valuesForSmsDayTables);

        setupDayTableValues(valuesForSmsDayTables, 0, TEST_VALUE_NO_OF_SUCCESSES, TEST_VALUE_MSS_EVNTSRC_ID, 5);
        insertRowOnSpecificConnection(connection1, TEMP_SMS_EVNTSRC_EVENTID_SUC_DAY, valuesForSmsDayTables);

        setupDayTableValues(valuesForSmsDayTables, 0, TEST_VALUE_NO_OF_SUCCESSES, TEST_VALUE_MSS_EVNTSRC_ID, 4);
        insertRowOnSpecificConnection(connection1, TEMP_SMS_EVNTSRC_EVENTID_SUC_DAY, valuesForSmsDayTables);

        setupDayTableValues(valuesForSmsDayTables, 0, TEST_VALUE_NO_OF_SUCCESSES, TEST_VALUE_MSS_EVNTSRC_ID, 5);
        insertRowOnSpecificConnection(connection1, TEMP_SMS_EVNTSRC_EVENTID_SUC_DAY, valuesForSmsDayTables);

    }

    private void createTemporaryICCTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(INTERNAL_CAUSE_CODE);
        columnsForTable.add(INTERNAL_CAUSE_CODE_DESC);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_MSS_INTERNAL_CAUSE_CODE, columnsForTable);
    }

    private void createTemporaryProtocolTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(EXTERNAL_PROTOCOL_ID);
        columnsForTable.add(EXTERNAL_PROTOCOL_NAME);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_MSS_PROTOCOL_CODE, columnsForTable);
    }

    private void createTemporaryRanapTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(RANAP_CODE);
        columnsForTable.add(RANAP_DESC);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_MSS_RANAP_CODE, columnsForTable);
    }

    private void createTemporaryBssmapTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(BSSMAP_CODE);
        columnsForTable.add(BSSMAP_DESC);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_MSS_BSSMAP_CODE, columnsForTable);
    }

    private void createTemporaryFaultCodeTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(FAULT_CODE);
        columnsForTable.add(FAULT_CODE_DESC);
        columnsForTable.add(ADVICE);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_MSS_FAULT_CODE, columnsForTable);
    }

    private void createTemporaryRatDespTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(RAT_COLUMN_NAME);
        columnsForTable.add(RAT_DESC);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_SGEH_RAT, columnsForTable);
    }

    private void insertRowIntoICCTable(final Connection connection1, final String table, final int internalCauseCode, final String iccDesc)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(INTERNAL_CAUSE_CODE, internalCauseCode);
        valuesForTable.put(INTERNAL_CAUSE_CODE_DESC, iccDesc);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void insertRowIntoProtocolTable(final Connection connection1, final String table, final int protocolID, final String protocolName)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(EXTERNAL_PROTOCOL_ID, protocolID);
        valuesForTable.put(EXTERNAL_PROTOCOL_NAME, protocolName);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void insertRowIntoRanapTable(final Connection connection1, final String table, final int ranapCode, final String ranapDesc)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(RANAP_CODE, ranapCode);
        valuesForTable.put(RANAP_DESC, ranapDesc);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void insertRowIntoBssmapTable(final Connection connection1, final String table, final int bssmapCode, final String bssmapDesc)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(BSSMAP_CODE, bssmapCode);
        valuesForTable.put(BSSMAP_DESC, bssmapDesc);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void insertRowIntoFaultCodeTable(final Connection connection1, final String table, final int faultCode, final String faultCodeDesc,
                                             final String advice) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(FAULT_CODE, faultCode);
        valuesForTable.put(FAULT_CODE_DESC, faultCodeDesc);
        valuesForTable.put(ADVICE, advice);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void insertRowIntoRatTable(final Connection connection1, final String table, final int causeProtoType, final String cptDesc)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(RAT_COLUMN_NAME, causeProtoType);
        valuesForTable.put(RAT_DESC, cptDesc);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void populateICCTable(final Connection connection1) throws SQLException {
        insertRowIntoICCTable(connection1, TEMP_DIM_E_MSS_INTERNAL_CAUSE_CODE, TEST_VALUE_INTERNAL_CAUSE_CODE, "Requested facility not subscribed");
    }

    private void populateProtocolTable(final Connection connection1) throws SQLException {
        insertRowIntoProtocolTable(connection1, TEMP_DIM_E_MSS_PROTOCOL_CODE, TEST_VALUE_EXTERNAL_PROTOCOL_ID, TEST_VALUE_EXTERNAL_PROTOCOL_NAME);
    }

    private void populateRanapTable(final Connection connection1) throws SQLException {
        insertRowIntoRanapTable(connection1, TEMP_DIM_E_MSS_RANAP_CODE, TEST_VALUE_RANAP_CODE, "Channel Unacceptable");
    }

    private void populateBssmapTable(final Connection connection1) throws SQLException {
        insertRowIntoBssmapTable(connection1, TEMP_DIM_E_MSS_BSSMAP_CODE, TEST_VALUE_BSSMAP_CODE, "Better Cell");
    }

    private void populateFaultCodeTable(final Connection connection1) throws SQLException {
        insertRowIntoFaultCodeTable(connection1, TEMP_DIM_E_MSS_FAULT_CODE, TEST_VALUE_FAULT_CODE,
                "Release message received with cause= no circuit available and diagnostic with CCBS not possible.", "advice");

    }

    private void populateRatDespTable(final Connection connection1) throws SQLException {
        insertRowIntoRatTable(connection1, TEMP_DIM_E_SGEH_RAT, RAT_FOR_GSM, "GSM");
    }

    private void createTemporaryEventSrcDespTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(EVENT_SOURCE_NAME);
        columnsForTable.add(EVNTSRC_ID);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_MSS_EVNTSRC, columnsForTable);
    }

    private void populateEventSrcTable(final Connection connection1) throws SQLException {
        insertRowIntoEventSrcTable(connection1, TEMP_DIM_E_MSS_EVNTSRC, TEST_VALUE_MSS_EVNTSRC, TEST_VALUE_MSS_EVNTSRC_ID);
    }

    private void insertRowIntoEventSrcTable(final Connection connection1, final String table, final String eventSrc, final long evntSrcId)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(EVENT_SOURCE_NAME, eventSrc);
        valuesForTable.put(EVNTSRC_ID, evntSrcId);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void createTemporaryHier321Table(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(RAT);
        columnsForTable.add(HIERARCHY_1);
        columnsForTable.add(HIERARCHY_3);
        columnsForTable.add(VENDOR_PARAM_UPPER_CASE);
        columnsForTable.add(HIER3_ID);
        columnsForTable.add(HIER321_ID);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_SGEH_HIER321, columnsForTable);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_Z_SGEH_HIER321, columnsForTable);
    }

    private void populateHier321Table(final Connection connection1) throws SQLException {
        insertRowIntoHier321Table(connection1, TEMP_DIM_E_SGEH_HIER321, RAT_FOR_GSM, TEST_VALUE_MSS_CELL_NAME, TEST_VALUE_MSS_CONTROLLER_NAME,
                ERICSSON, TEST_VALUE_MSS_HIER3_ID, TEST_VALUE_MSS_HIER321_ID);
    }

    private void insertRowIntoHier321Table(final Connection connection1, final String table, final int rat, final String cell,
                                           final String controller, final String vendor, final long hier3Id, final long hier321Id)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(RAT, rat);
        valuesForTable.put(HIERARCHY_1, cell);
        valuesForTable.put(HIERARCHY_3, controller);
        valuesForTable.put(VENDOR_PARAM_UPPER_CASE, vendor);
        valuesForTable.put(HIER3_ID, hier3Id);
        valuesForTable.put(HIER321_ID, hier321Id);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void createTemporaryEventTypeTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(EVENT_ID);
        columnsForTable.add(com.ericsson.eniq.events.server.common.ApplicationConstants.EVENT_ID_DESC);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_MSS_EVENTTYPE, columnsForTable);
    }

    private void populateEventTypeTable(final Connection connection1) throws SQLException {
        insertRowIntoEventTypeTable(connection1, TEMP_DIM_E_MSS_EVENTTYPE, MSS_MS_ORIGINATING_EVENT_ID, MS_ORIGINATING);
        insertRowIntoEventTypeTable(connection1, TEMP_DIM_E_MSS_EVENTTYPE, MSS_MS_TERMINATING_EVENT_ID, MS_TERMINATING);
        insertRowIntoEventTypeTable(connection1, TEMP_DIM_E_MSS_EVENTTYPE, MSS_CALL_FORWARDING_EVENT_ID, CALL_FORWARDING);
        insertRowIntoEventTypeTable(connection1, TEMP_DIM_E_MSS_EVENTTYPE, MSS_ROAMING_CALL_EVENT_ID, ROAMING_CALL_FORWARDING);
        insertRowIntoEventTypeTable(connection1, TEMP_DIM_E_MSS_EVENTTYPE, MSS_LOCATION_SERVICE_EVENT_ID, LOCATION_SERVICES);
        insertRowIntoEventTypeTable(connection1, TEMP_DIM_E_MSS_EVENTTYPE, MSS_SMS_MS_ORIGINATING_EVENT_ID, MS_ORIGINATING_SMS_MSC);
        insertRowIntoEventTypeTable(connection1, TEMP_DIM_E_MSS_EVENTTYPE, MSS_SMS_MS_TERMINATING_EVENT_ID, MS_TERMINATING_SMS_MSC);
    }

    private void insertRowIntoEventTypeTable(final Connection connection1, final String table, final String evntId, final String evntDesc)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(EVENT_ID, evntId);
        valuesForTable.put(com.ericsson.eniq.events.server.common.ApplicationConstants.EVENT_ID_DESC, evntDesc);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void createTemporaryTacTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(TAC);
        columnsForTable.add(MANUFACTURER);
        columnsForTable.add(MARKETING_NAME);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_SGEH_TAC, columnsForTable);
    }

    private void populateTacTable(final Connection connection1) throws SQLException {
        insertRowIntoTacTable(connection1, TEMP_DIM_E_SGEH_TAC, TEST_VALUE_TAC, "Jixiangjia Group Co Ltd", "TIGI TG101, TG102, TG106, TG108");
        insertRowIntoTacTable(connection1, TEMP_DIM_E_SGEH_TAC, TEST_VALUE_EXCLUSIVE_TAC, "Jixiangjia Group Co Ltd",
                "TIGI TG101, TG102, TG106, TG108");
    }

    private void insertRowIntoTacTable(final Connection connection1, final String table, final String tac, final String manuf,
                                       final String marketingName) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(TAC, tac);
        valuesForTable.put(MANUFACTURER, manuf);
        valuesForTable.put(MARKETING_NAME, marketingName);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void createTemporaryLcsclienttypeTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(LCS_CLIENT_TYPE);
        columnsForTable.add(LCS_CLIENT_TYPE_DESC);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_MSS_LCS_CLIENT_TYPE, columnsForTable);
    }

    private void populateLcsclienttypeTable(final Connection connection1) throws SQLException {
        insertRowIntoLcsclienttypeTable(connection1, TEMP_DIM_E_MSS_LCS_CLIENT_TYPE, TEST_VALUE_LCS_CLIENT_TYPE, "valueAddedServices");
    }

    private void insertRowIntoLcsclienttypeTable(final Connection connection1, final String table, final int lcsClientType,
                                                 final String lcsClientTypeDesc) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(LCS_CLIENT_TYPE, lcsClientType);
        valuesForTable.put(LCS_CLIENT_TYPE_DESC, lcsClientTypeDesc);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void createTemporaryTypelocationreqTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(TYPE_LOCATION_REQ);
        columnsForTable.add(TYPE_LOCATION_REQ_DESC);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_MSS_TYPE_LOCATION_REQ, columnsForTable);
    }

    private void populateTypelocationreqTable(final Connection connection1) throws SQLException {
        insertRowIntoTypelocationreqTable(connection1, TEMP_DIM_E_MSS_TYPE_LOCATION_REQ, TEST_VALUE_TYPE_LOCATION_REQ,
                "mO_LocationRequestLocEstimateToMS");
    }

    private void insertRowIntoTypelocationreqTable(final Connection connection1, final String table, final int typeLocReq, final String typeLocReqDesc)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(TYPE_LOCATION_REQ, typeLocReq);
        valuesForTable.put(TYPE_LOCATION_REQ_DESC, typeLocReqDesc);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void createTemporaryUnSucPosReasonTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(UNSUC_POSITION_REASON);
        columnsForTable.add(UNSUC_POSITION_REASON_DESC);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_MSS_UNSUC_POSITION_REASON, columnsForTable);
    }

    private void populateUnSucPosReasonTable(final Connection connection1) throws SQLException {
        insertRowIntoUnSucPosReasonTable(connection1, TEMP_DIM_E_MSS_UNSUC_POSITION_REASON, TEST_VALUE_UNSUC_POSITION_REASON,
                "userDeniedDueToPrivacyVerification");
    }

    private void insertRowIntoUnSucPosReasonTable(final Connection connection1, final String table, final int unSucPosReason,
                                                  final String unSucPosReasonDesc) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(UNSUC_POSITION_REASON, unSucPosReason);
        valuesForTable.put(UNSUC_POSITION_REASON_DESC, unSucPosReasonDesc);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void createTemporaryGroupEvntSrcTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(GROUP_NAME);
        columnsForTable.add(EVENT_SOURCE_NAME);
        columnsForTable.add(EVNTSRC_ID);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_GROUP_TYPE_E_EVNTSRC_CS, columnsForTable);
    }

    private void populateGroupEvntSrcTable(final Connection connection1) throws SQLException {
        insertRowIntoGroupEvntSrcTable(connection1, TEMP_GROUP_TYPE_E_EVNTSRC_CS, TEST_VALUE_MSS_GROUP_EVNTSRC, TEST_VALUE_MSS_EVNTSRC,
                TEST_VALUE_MSS_EVNTSRC_ID);
    }

    private void insertRowIntoGroupEvntSrcTable(final Connection connection1, final String table, final String groupName, final String evntSrcName,
                                                final long hier3Id) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(GROUP_NAME, groupName);
        valuesForTable.put(EVENT_SOURCE_NAME, evntSrcName);
        valuesForTable.put(EVNTSRC_ID, hier3Id);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void createTemporaryGroupControllerTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(GROUP_NAME);
        columnsForTable.add(RAT);
        columnsForTable.add(VENDOR_PARAM_UPPER_CASE);
        columnsForTable.add(HIERARCHY_3);
        columnsForTable.add(HIER3_ID);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, columnsForTable);
    }

    private void populateGroupControllerTable(final Connection connection1) throws SQLException {
        insertRowIntoGroupControllerTable(connection1, TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, TEST_VALUE_MSS_GROUP_CONTROLLER, RAT_FOR_GSM, ERICSSON,
                TEST_VALUE_MSS_CONTROLLER_NAME, TEST_VALUE_MSS_HIER3_ID);
    }

    private void insertRowIntoGroupControllerTable(final Connection connection1, final String table, final String groupName, final int rat,
                                                   final String vendor, final String controllerName, final long hier3Id) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(GROUP_NAME, groupName);
        valuesForTable.put(RAT, rat);
        valuesForTable.put(VENDOR_PARAM_UPPER_CASE, vendor);
        valuesForTable.put(HIERARCHY_3, controllerName);
        valuesForTable.put(HIER3_ID, hier3Id);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void createTemporaryGroupCellTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(GROUP_NAME);
        columnsForTable.add(RAT);
        columnsForTable.add(VENDOR_PARAM_UPPER_CASE);
        columnsForTable.add(HIERARCHY_3);
        columnsForTable.add(HIERARCHY_1);
        columnsForTable.add(HIER3_ID);
        columnsForTable.add(HIER321_ID);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, columnsForTable);
    }

    private void populateGroupCellTable(final Connection connection1) throws SQLException {
        insertRowIntoGroupCellTable(connection1, TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, TEST_VALUE_MSS_GROUP_CELL, RAT_FOR_GSM, ERICSSON,
                TEST_VALUE_MSS_CONTROLLER_NAME, TEST_VALUE_MSS_CELL_NAME, TEST_VALUE_MSS_HIER3_ID, TEST_VALUE_MSS_HIER321_ID);
    }

    private void insertRowIntoGroupCellTable(final Connection connection1, final String table, final String groupName, final int rat,
                                             final String vendor, final String controllerName, final String cellName, final long hier3Id,
                                             final long hier321Id) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(GROUP_NAME, groupName);
        valuesForTable.put(RAT, rat);
        valuesForTable.put(VENDOR_PARAM_UPPER_CASE, vendor);
        valuesForTable.put(HIERARCHY_3, controllerName);
        valuesForTable.put(HIERARCHY_1, cellName);
        valuesForTable.put(HIER3_ID, hier3Id);
        valuesForTable.put(HIER321_ID, hier321Id);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void populateGroupTacTable(final Connection connection1) throws SQLException {
        insertRowIntoGroupTacTable(connection1, TEMP_GROUP_TYPE_E_TAC, TEST_VALUE_TAC_GROUP, TEST_VALUE_TAC);
        insertRowIntoGroupTacTable(connection1, TEMP_GROUP_TYPE_E_TAC, EXCLUSIVE_TAC_GROUP, TEST_VALUE_EXCLUSIVE_TAC);
    }

    private void insertRowIntoGroupTacTable(final Connection connection1, final String table, final String grpName, final String tac)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(GROUP_NAME, grpName);
        valuesForTable.put(TAC, tac);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void createTemporaryGroupImsiTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(GROUP_NAME);
        columnsForTable.add(IMSI);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_GROUP_TYPE_E_IMSI, columnsForTable);
    }

    private void populateGroupImsiTable(final Connection connection1) throws SQLException {
        insertRowIntoGroupImsiTable(connection1, TEMP_GROUP_TYPE_E_IMSI, TEST_VALUE_IMSI_GROUP, TEST_VALUE_IMSI);
    }

    private void insertRowIntoGroupImsiTable(final Connection connection1, final String table, final String grpName, final long imsi)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(GROUP_NAME, grpName);
        valuesForTable.put(IMSI, imsi);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void createTemporarySmsResultTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(SMS_RESULT);
        columnsForTable.add(SMS_RESULT_DESC);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_MSS_SMS_RESULT, columnsForTable);
    }

    private void populateSmsResultTable(final Connection connection1) throws SQLException {
        insertRowIntoSmsResultTable(connection1, TEMP_DIM_E_MSS_SMS_RESULT, TEST_VALUE_SMS_RESULT, "unsuccessfulMOSMSDeliverytoSMSCDuetoCAMELReason");
    }

    private void insertRowIntoSmsResultTable(final Connection connection1, final String table, final int smsResult, final String smsResultDesc)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(SMS_RESULT, smsResult);
        valuesForTable.put(SMS_RESULT_DESC, smsResultDesc);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void createTemporaryMsgTypeIndicatorTable(final Connection connection1) throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(MSG_TYPE_INDICATOR);
        columnsForTable.add(MSG_TYPE_INDICATOR_DESC);
        new SQLCommand(connection1).createTemporaryTableMss(TEMP_DIM_E_MSS_MSG_TYPE_INDICATOR, columnsForTable);
    }

    private void populateMsgTypeIndicatorTable(final Connection connection1) throws SQLException {
        insertRowIntoMsgTypeIndicatorTable(connection1, TEMP_DIM_E_MSS_MSG_TYPE_INDICATOR, TEST_VALUE_MSG_TYPE_INDICATOR, "sMSdeliverSCtoMS");
    }

    private void insertRowIntoMsgTypeIndicatorTable(final Connection connection1, final String table, final int msgTypeIndicator,
                                                    final String msgTypeIndicatorDesc) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(MSG_TYPE_INDICATOR, msgTypeIndicator);
        valuesForTable.put(MSG_TYPE_INDICATOR_DESC, msgTypeIndicatorDesc);
        new SQLCommand(connection1).insertRow(table, valuesForTable);
    }

    private void createVoiceTempTopolgyTables(final Connection connection1) throws Exception {
        commonTemporaryTopologyTables(connection1);
        voiceSpecificTemporaryTopologyTables(connection1);
    }

    private void createLocServiceTopologyTables(final Connection connection1) throws Exception {
        commonTemporaryTopologyTables(connection1);
        locServiceSpecificTemporaryTopologyTables(connection1);
    }

    private void createSmsTopologyTables(final Connection connection1) throws Exception {
        commonTemporaryTopologyTables(connection1);
        smsSpecificTemporaryTopologyTables(connection1);
    }

    private void createGroupTemporaryTopologyTables(final Connection connection1) throws Exception {
        createTemporaryGroupEvntSrcTable(connection1);
        populateGroupEvntSrcTable(connection1);

        createTemporaryGroupControllerTable(connection1);
        populateGroupControllerTable(connection1);

        createTemporaryGroupCellTable(connection1);
        populateGroupCellTable(connection1);

        createTemporaryGroupImsiTable(connection1);
        populateGroupImsiTable(connection1);
    }

    private void commonTemporaryTopologyTables(final Connection connection1) throws Exception {
        createTemporaryRatDespTable(connection1);
        populateRatDespTable(connection1);

        createTemporaryEventSrcDespTable(connection1);
        populateEventSrcTable(connection1);

        createTemporaryHier321Table(connection1);
        populateHier321Table(connection1);

        createTemporaryEventTypeTable(connection1);
        populateEventTypeTable(connection1);

        createTemporaryTacTable(connection1);
        populateTacTable(connection1);

        populateGroupTacTable(connection1);
    }

    private void voiceSpecificTemporaryTopologyTables(final Connection connection1) throws Exception {
        createTemporaryICCTable(connection1);
        populateICCTable(connection1);

        createTemporaryProtocolTable(connection1);
        populateProtocolTable(connection1);

        createTemporaryRanapTable(connection1);
        populateRanapTable(connection1);

        createTemporaryBssmapTable(connection1);
        populateBssmapTable(connection1);

        createTemporaryFaultCodeTable(connection1);
        populateFaultCodeTable(connection1);

    }

    private void locServiceSpecificTemporaryTopologyTables(final Connection connection1) throws Exception {
        createTemporaryLcsclienttypeTable(connection1);
        populateLcsclienttypeTable(connection1);

        createTemporaryTypelocationreqTable(connection1);
        populateTypelocationreqTable(connection1);

        createTemporaryUnSucPosReasonTable(connection1);
        populateUnSucPosReasonTable(connection1);

    }

    private void smsSpecificTemporaryTopologyTables(final Connection connection1) throws Exception {
        createTemporarySmsResultTable(connection1);
        populateSmsResultTable(connection1);

        createTemporaryMsgTypeIndicatorTable(connection1);
        populateMsgTypeIndicatorTable(connection1);

    }

    /**
     * This method will create another db connection and add it to the interceptedDbConnectionManager. This is necessary if the code will require a
     * second db request because DataServiceBean will close the connection after each query, e.g. the code will verify if a TAC is in the
     * EXCLUSIVE_TAC group before sending an event analysis.
     * 
     * @throws Exception
     */
    protected Connection addAndReturnConnectionToInterceptedDbConnectionManager() throws Exception {
        final Connection anotherConnection = getDWHDataSourceConnection();
        createTemporaryTableOnSpecificConnection(anotherConnection, TEMP_GROUP_TYPE_E_TAC, groupColumns);
        interceptedDbConnectionManager.addConnection(anotherConnection);
        return anotherConnection;
    }

    private void createNPopulateSummaryTables() throws Exception {

        createNPopulateErrTempTablesSms(this.connection);
        createNPopulateSucTempTablesSms(this.connection);
        commonTemporaryTopologyTables(this.connection);

        final Connection connection1 = addAndReturnConnectionToInterceptedDbConnectionManager();
        createNPopulateErrTempTablesLocServices(connection1);
        createNPopulateSucTempTablesLocServices(connection1);
        commonTemporaryTopologyTables(connection1);

        final Connection connection2 = addAndReturnConnectionToInterceptedDbConnectionManager();
        createNPopulateErrTempTablesVoice(connection2);
        createNPopulateSucTempTablesVoice(connection2);
        commonTemporaryTopologyTables(connection2);
    }

    private void createNPopulateSummaryTablesWeek() throws Exception {
        createNPopulateErrTempTablesSmsWeek(this.connection);
        createNPopulateSucTempTablesSmsWeek(this.connection);
        commonTemporaryTopologyTables(this.connection);
        createNPopulateErrTempTablesSms(this.connection);
        createNPopulateSucTempTablesSms(this.connection);

        final Connection connection2 = addAndReturnConnectionToInterceptedDbConnectionManager();
        createNPopulateErrTempTablesLocServicesWeek(connection2);
        createNPopulateSucTempTablesLocServicesWeek(connection2);
        createNPopulateErrTempTablesLocServices(connection2);
        createNPopulateSucTempTablesLocServices(connection2);
        commonTemporaryTopologyTables(connection2);

        final Connection connection3 = addAndReturnConnectionToInterceptedDbConnectionManager();
        createNPopulateErrTempTablesVoiceWeek(connection3);
        createNPopulateSucTempTablesVoiceWeek(connection3);
        createNPopulateErrTempTablesVoice(connection3);
        createNPopulateSucTempTablesVoice(connection3);
        commonTemporaryTopologyTables(connection3);
    }

    private void createNPopulateGroupSummaryTables() throws Exception {
        createNPopulateErrTempTablesSms(this.connection);
        createNPopulateSucTempTablesSms(this.connection);
        commonTemporaryTopologyTables(this.connection);
        createGroupTemporaryTopologyTables(this.connection);

        final Connection connection1 = addAndReturnConnectionToInterceptedDbConnectionManager();
        createNPopulateErrTempTablesLocServices(connection1);
        createNPopulateSucTempTablesLocServices(connection1);
        commonTemporaryTopologyTables(connection1);
        createGroupTemporaryTopologyTables(connection1);

        final Connection connection2 = addAndReturnConnectionToInterceptedDbConnectionManager();
        createNPopulateErrTempTablesVoice(connection2);
        createNPopulateSucTempTablesVoice(connection2);
        commonTemporaryTopologyTables(connection2);
        createGroupTemporaryTopologyTables(connection2);
    }
}
