/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.DataServiceBaseTestCase;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author egraman
 * @since 2011
 *
 */
public class MssBaseResourceIntegrationTest extends DataServiceBaseTestCase {

    private MssBaseResource mssBaseResource;

    private final static List<String> keys = new ArrayList<String>();

    static {
        keys.add(KEY_TYPE_ERR);
        keys.add(KEY_TYPE_SUC);
        keys.add(KEY_TYPE_DROP_CALL);
        keys.add(KEY_TYPE_LOC_SERVICE_ERR);
        keys.add(KEY_TYPE_LOC_SERVICE_SUC);
        keys.add(KEY_TYPE_SMS_ERR);
        keys.add(KEY_TYPE_SMS_SUC);
    }

    @Override
    protected void onSetUp() throws Exception {
        super.onSetUp();
        mssBaseResource = new ExtendedBaseResource();
        attachDependencies(mssBaseResource);
    }

    @Test
    public void testqueryTimeRangeViewsForRawTableNames() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TIME_QUERY_PARAM, "10080");
        requestParameters.putSingle(TZ_OFFSET, "+0100");
        final FormattedDateTimeRange dateTimeRange = mssBaseResource.getFormattedDateTimeRange(requestParameters, null);
        for (final String key : keys) {
            mssBaseResource.getRAWTablesUsingQuery(dateTimeRange, key, RAW_MSS_TABLES);
        }
    }

    class ExtendedBaseResource extends MssBaseResource {

        @Override
        protected String getData(final String requestID, final MultivaluedMap<String, String> requestParameters)
                throws WebApplicationException {

            return null;
        }

        @Override
        protected List<String> checkParameters(final MultivaluedMap<String, String> requestParameters) {

            return null;
        }

        /* (non-Javadoc)
         * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
         */
        @Override
        protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {

            return false;
        }

    }

}
