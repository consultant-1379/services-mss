/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.DataServiceBaseTestCase;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author egraman
 * @since 2011
 *
 */
public class CauseCodeTablesICCResourceIntegrationTest extends DataServiceBaseTestCase {

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String MAX_ROWS_VALUE = "500";

    private MultivaluedMap<String, String> map;

    private InternalCauseCodeTablesICCResource internalCauseCodeTablesICCResource;

    private InternalCauseCodeTablesFCResource internalCauseCodeTablesFCResource;

    @Override
    public void onSetUp() {
        internalCauseCodeTablesICCResource = new InternalCauseCodeTablesICCResource();

        attachDependencies(internalCauseCodeTablesICCResource);

        internalCauseCodeTablesFCResource = new InternalCauseCodeTablesFCResource();

        attachDependencies(internalCauseCodeTablesFCResource);

        map = new MultivaluedMapImpl();
    }

    public void testGetIccTable() throws Exception {
        // MSS/NETWORK/CAUSE_CODE_ANALYSIS/TABLE_ICC?display=grid&tzOffset=+0100&maxRows=500
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TZ_OFFSET, "+0000");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = internalCauseCodeTablesICCResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    public void testGetFcTable() throws Exception {
        // MSS/NETWORK/CAUSE_CODE_ANALYSIS/TABLE_FC?display=grid&tzOffset=+0100&maxRows=500
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TZ_OFFSET, "+0000");
        final String result = internalCauseCodeTablesFCResource.getData("requestID", map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

}
