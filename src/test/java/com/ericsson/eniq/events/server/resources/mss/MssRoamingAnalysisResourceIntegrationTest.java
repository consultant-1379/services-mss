/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import java.net.URISyntaxException;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.common.MessageConstants;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ezhbhe
 * @since Jun 2011
 */
public class MssRoamingAnalysisResourceIntegrationTest extends MssDataServiceBaseTestCase {

    private MultivaluedMap<String, String> map;

    private MssRoamingAnalysisResource mssRoamingAnalysisResource;

    private static final String DISPLAY_TYPE = CHART_PARAM;

    private static final String TIME_5_MINS = "5";//TR1 (RAW Tables)

    private static final String DATE_FROM_RAW = "14062011";

    private static final String DATE_TO_RAW = "10082011";

    private static final String TIME_FROM_RAW = "2000";

    private static final String TIME_TO_RAW = "2005";

    @Override
    public void onSetUp() {
        mssRoamingAnalysisResource = new MssRoamingAnalysisResource();
        attachDependenciesForMssBaseResource(mssRoamingAnalysisResource);
        mssRoamingAnalysisResource.setQueryConstructor(this.queryConstructor);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetData() throws URISyntaxException {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, TIME_5_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        DummyUriInfoImpl.setUriInfoForMss(map, mssRoamingAnalysisResource, SAMPLE_BASE_URI,
                "MSS/NETWORK/ROAMING_ANALYSIS/OPERATOR");
        boolean throwUnSupportedOperationException = false;
        try {
            mssRoamingAnalysisResource.getData("CANCEL_REQUEST_NOT_SUPPORTED", map);
        } catch (final UnsupportedOperationException e) {
            throwUnSupportedOperationException = true;
        }
        assertTrue(throwUnSupportedOperationException);
    }

    @Test
    public void testNoDisplayType() {
        map.clear();

        map.putSingle(TIME_QUERY_PARAM, TIME_5_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        final String result = mssRoamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_OPERATOR, map);
        assertTrue(result.contains(DISPLAY_PARAM));
    }

    @Test
    public void testInvalidDisplayType() {
        final String invalidDisplayType = "error";
        map.clear();

        map.putSingle(DISPLAY_PARAM, invalidDisplayType);
        map.putSingle(TIME_QUERY_PARAM, TIME_5_MINS);
        map.putSingle(TZ_OFFSET, "+0100");
        final String result = mssRoamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_OPERATOR, map);
        assertTrue(result.contains(MessageConstants.E_NO_SUCH_DISPLAY_TYPE));
    }

    @Test
    public void testRoamingAnalysisForOperatorByTimerangeRAW() throws URISyntaxException {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM_RAW);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO_RAW);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM_RAW);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO_RAW);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(ROAMING_OBJECT, ROAMING_OPERATOR);

        DummyUriInfoImpl.setUriInfoForMss(map, mssRoamingAnalysisResource, SAMPLE_BASE_URI,
                "MSS/NETWORK/ROAMING_ANALYSIS/OPERATOR");
        final String result = mssRoamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_OPERATOR, map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testRoamingAnalysisForCountryByTimerangeRAW() throws URISyntaxException {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM_RAW);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO_RAW);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM_RAW);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO_RAW);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(ROAMING_OBJECT, ROAMING_COUNTRY);

        DummyUriInfoImpl.setUriInfoForMss(map, mssRoamingAnalysisResource, SAMPLE_BASE_URI,
                "MSS/NETWORK/ROAMING_ANALYSIS/COUNTRY");
        final String result = mssRoamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_COUNTRY, map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testRoamingAnalysisForCountryByTimerange1WeekRAW() throws URISyntaxException {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(ROAMING_OBJECT, ROAMING_COUNTRY);

        DummyUriInfoImpl.setUriInfoForMss(map, mssRoamingAnalysisResource, SAMPLE_BASE_URI,
                "MSS/NETWORK/ROAMING_ANALYSIS/COUNTRY");
        final String result = mssRoamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_COUNTRY, map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testRoamingAnalysisForOperatorByTimerange30minRAW() throws URISyntaxException {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(ROAMING_OBJECT, ROAMING_OPERATOR);

        DummyUriInfoImpl.setUriInfoForMss(map, mssRoamingAnalysisResource, SAMPLE_BASE_URI,
                "MSS/NETWORK/ROAMING_ANALYSIS/OPERATOR");
        final String result = mssRoamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_OPERATOR, map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

}
