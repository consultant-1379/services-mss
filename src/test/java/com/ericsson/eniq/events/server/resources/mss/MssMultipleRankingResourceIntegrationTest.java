/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import java.net.URISyntaxException;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.junit.Test;

import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author egraman
 * @author echimma
 * @author ezhibhe
 * @since 2011
 * 
 */
public class MssMultipleRankingResourceIntegrationTest extends MssDataServiceBaseTestCase {

    private MultivaluedMap<String, String> map;

    private MssMultipleRankingResource mssMultipleRankingResource;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String MAX_ROWS_VALUE = "500";

    private static final String TIME_5_MINS = "5";//TR1 (RAW Tables)

    private static final String TIME_30_MINS = "30";//TR2 (1MIN --> RAW)

    private static final String TIME_6_HOURS = "360";//TR3 (15MIN --> Aggregation table)

    private static final String TIME_8_DAYS = "11520";//TR4 (1DAY --> Aggregation table)

    private static final String DROP_CALLS_URI = "MSS/NETWORK/RANKING_ANALYSIS/VOICE/DROPPED_CALLS";

    private static final String TOTAL_CALLS_URI = "MSS/NETWORK/RANKING_ANALYSIS/VOICE/TOTAL_CALLS";

    private static final String BLOCKED_CALLS_URI = "MSS/NETWORK/RANKING_ANALYSIS/VOICE/BLOCKED_CALLS";

    private static final String LONG_DURATION_CALLS_URI = "MSS/NETWORK/RANKING_ANALYSIS/VOICE/LONG_DURATION_CALLS";

    private static final String SHORT_DURATION_CALLS_URI = "MSS/NETWORK/RANKING_ANALYSIS/VOICE/SHORT_DURATION_CALLS";

    private static final String MS_ORIGINATING_UNANSWERED_URI = "MSS/NETWORK/RANKING_ANALYSIS/VOICE/MS_ORIGINATING_UNANSWERED";

    private static final String MS_TERMINATING_UNANSWERED_URI = "MSS/NETWORK/RANKING_ANALYSIS/VOICE/MS_TERMINATING_UNANSWERED";

    private static final String TZ_OFFSET_VALUE = "+0100";

    @Override
    public void onSetUp() {
        mssMultipleRankingResource = new MssMultipleRankingResource();
        attachDependenciesForMssBaseResource(mssMultipleRankingResource);
        map = new MultivaluedMapImpl();
    }

    private void setMapParametersWithTimeAndType(final MultivaluedMap<String, String> map, final String type,
            final String time) {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, type);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_VALUE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
    }

    @Test
    public void testGetICCRankingDataRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_INTERNAL_CAUSE_CODE, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, TOTAL_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, TOTAL_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetICCRankingDataAgg15Min() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_INTERNAL_CAUSE_CODE, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, TOTAL_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, TOTAL_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetICCRankingDataAgg1Day() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_INTERNAL_CAUSE_CODE, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, TOTAL_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, TOTAL_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testIMSIRankingRaw() throws Exception {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, TOTAL_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataForBlockedCallsRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataForDroppedCallsRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataForBlockedCalls30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataForDroppedCalls30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataForBlockedCallsAggr15MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataForDroppedCallsAggr15MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataForBlockedCallsAggr1DAY() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataForDroppedCallsAggr1DAY() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetBSCRankingDataForBlockedCallsRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_BSC, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetBSCRankingDataForDroppedCallsRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_BSC, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetBSCRankingDataForBlockedCalls30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_BSC, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetBSCRankingDataForDroppedCalls30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_BSC, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetBSCRankingDataForBlockedCallsAggr15MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_BSC, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetBSCRankingDataForDroppedCallsAggr15MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_BSC, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetBSCRankingDataForBlockedCallsAggr1DAY() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_BSC, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetBSCRankingDataForDroppedCallsAggr1DAY() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_BSC, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRNCRankingDataForBlockedCallsRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_RNC, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRNCRankingDataForDroppedCallsRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_RNC, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRNCRankingDataForBlockedCalls30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_RNC, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRNCRankingDataForDroppedCalls30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_RNC, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRNCRankingDataForBlockedCallsAggr15MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_RNC, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRNCRankingDataForDroppedCallsAggr15MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_RNC, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRNCRankingDataForBlockedCallsAggr1DAY() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_RNC, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRNCRankingDataForDroppedCallsAggr1DAY() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_RNC, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetCELLRankingDataForBlockedCallsRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_CELL, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetCELLRankingDataForDroppedCallsRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_CELL, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetCELLRankingDataForBlockedCalls30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_CELL, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetCELLRankingDataForDroppedCalls30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_CELL, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetCELLRankingDataForBlockedCallsAggr15MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_CELL, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetCELLRankingDataForDroppedCallsAggr15MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_CELL, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetCELLRankingDataForBlockedCallsAggr1DAY() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_CELL, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetCELLRankingDataForDroppedCallsAggr1DAY() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_CELL, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTACRankingDataForBlockedCallsRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_TAC, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTACRankingDataForDroppedCallsRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_TAC, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTACRankingDataForBlockedCalls30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_TAC, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTACRankingDataForDroppedCalls30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_TAC, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTACRankingDataForBlockedCallsAggr15MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_TAC, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTACRankingDataForDroppedCallsAggr15MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_TAC, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTACRankingDataForBlockedCallsAggr1DAY() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_TAC, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTACRankingDataForDroppedCallsAggr1DAY() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_TAC, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetMSCRankingDataForBlockedCallsRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_MSC, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetMSCRankingDataForDroppedCallsRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_MSC, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetMSCRankingDataForBlockedCalls30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_MSC, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetMSCRankingDataForDroppedCalls30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_MSC, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetMSCRankingDataForBlockedCallsAgg15Min() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_MSC, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetMSCRankingDataForDroppedCallsAgg15Min() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_MSC, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetMSCRankingDataForBlockedCallsAgg1Day() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_MSC, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, BLOCKED_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, BLOCKED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetMSCRankingDataForDroppedCallsAgg1Day() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_MSC, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, DROP_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataForLongDurationCallsRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, LONG_DURATION_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, LONG_DURATION_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataForLongDurationCalls30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, LONG_DURATION_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, LONG_DURATION_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataForLongDurationCallsAgg15MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, LONG_DURATION_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, LONG_DURATION_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataForShortDurationCallsRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, SHORT_DURATION_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, SHORT_DURATION_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataForShortDurationCalls30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, SHORT_DURATION_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, SHORT_DURATION_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataForShortDurationCalls15MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, SHORT_DURATION_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, SHORT_DURATION_CALLS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataMsOriginatingForRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI,
                MS_ORIGINATING_UNANSWERED_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, MS_ORIGINATING_UNANSWERED);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataMsOriginatingFor30MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_30_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI,
                MS_ORIGINATING_UNANSWERED_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, MS_ORIGINATING_UNANSWERED);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataMsOriginatingFor15MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI,
                MS_ORIGINATING_UNANSWERED_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, MS_ORIGINATING_UNANSWERED);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataMsTerminatingForRAW() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI,
                MS_TERMINATING_UNANSWERED_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, MS_TERMINATING_UNANSWERED);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetIMSIRankingDataMsTerminatingFor15MIN() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_6_HOURS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI,
                MS_TERMINATING_UNANSWERED_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, MS_TERMINATING_UNANSWERED);
        assertJSONSucceeds(result);
    }

    @Test
    public void testInvidateParametersErrorDisplay() throws URISyntaxException {
        map.clear();

        map.putSingle(DISPLAY_PARAM, "");
        map.putSingle(TYPE_PARAM, IMSI_PARAM_UPPER_CASE);
        map.putSingle(TIME_QUERY_PARAM, TIME_30_MINS);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_VALUE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, TOTAL_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assert (result.contains("No such display type : "));
    }

    @Test
    public void testInvidateParametersMissDisplay() throws URISyntaxException {
        map.clear();

        map.putSingle(TYPE_PARAM, IMSI_PARAM_UPPER_CASE);
        map.putSingle(TIME_QUERY_PARAM, TIME_30_MINS);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_VALUE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, TOTAL_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assert (result.contains("Invalid or missing parameter(s) : display"));
    }

    @Test
    public void testInvidateParametersMissType() throws URISyntaxException {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, TIME_30_MINS);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_VALUE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, TOTAL_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, DROPPED_CALLS);
        assert (result.contains("Invalid or missing parameter(s) : type"));
    }

    @Test
    public void testErrorResponseForCallType() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_CELL, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI,
                "MSS/NETWORK/RANKING_ANALYSIS/VOICE1/DROPPED_CALLS");
        boolean rst = false;
        try {
            mssMultipleRankingResource.getData("VOICE1", DROPPED_CALLS);
        } catch (final UnsupportedOperationException e) {
            rst = true;
        }
        assertTrue(rst);
    }

    @Test
    public void testErrorResponseForErrorType() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, TYPE_CELL, TIME_8_DAYS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI,
                "MSS/NETWORK/RANKING_ANALYSIS/VOICE/DROPPED1_CALLS");
        boolean rst = false;
        try {
            mssMultipleRankingResource.getData(VOICE, "DROPPED1_CALLS");
        } catch (final UnsupportedOperationException e) {
            rst = true;
        }
        assertTrue(rst);
    }

    @Test
    public void testIsValidValue() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_30_MINS);
        map.putSingle(ERROR_TYPE_PARAM, TOTAL_CALLS_URI);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, TOTAL_CALLS_URI);
        boolean result = false;
        try {
            mssMultipleRankingResource.isValidValue(map);
        } catch (final UnsupportedOperationException e) {
            result = true;
        }
        assertTrue(result);
    }

    @Test
    public void testGetDataAsCSV() throws URISyntaxException {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_5_MINS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, TOTAL_CALLS_URI);
        final Response r = mssMultipleRankingResource.getDataAsCSV(VOICE, DROPPED_CALLS);
        assertNotNull(r);
    }

    @Test
    public void testGetDataWithRequestID() {
        setMapParametersWithTimeAndType(map, IMSI_PARAM_UPPER_CASE, TIME_30_MINS);
        boolean result = false;
        try {
            mssMultipleRankingResource.getData(REQUEST_ID, map);
        } catch (final UnsupportedOperationException e) {
            result = true;
        }
        assertTrue(result);
    }

    @Test
    public void testErrorResponseForTypeIMSITotalCalls() throws Exception {
        setMapParametersWithTimeAndType(map, TYPE_IMSI, TIME_30_MINS);
        map.putSingle(ERROR_TYPE_PARAM, TOTAL_CALLS);

        DummyUriInfoImpl.setUriInfoForMss(map, mssMultipleRankingResource, SAMPLE_BASE_URI, TOTAL_CALLS_URI);
        final String result = mssMultipleRankingResource.getData(VOICE, TOTAL_CALLS);
        assert (result.contains("Invalid or missing parameter(s) : type"));
    }
}
