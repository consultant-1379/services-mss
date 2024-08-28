package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryCreatorTestHelper.*;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.queryconstructor.QueryConstructorBaseTestClass;

public class MssEventVolumeQueryImsubCreatorTest extends QueryConstructorBaseTestClass {

    private MssEventVolumeQueryImsubCreator testObj;

    private Map<String, Object> params;

    @Before
    public void onSetUp() {
        testObj = new MssEventVolumeQueryImsubCreator();
        testObj.setTemplateUtils(this.templateUtils);
    }

    @Test
    public void testCreateQueryForNoTypeRaw() {
        params = new HashMap<String, Object>();
        final String timerange = RAW;
        final String type = NO_TYPE;
        params.put(TYPE_PARAM, type);
        params.put(TIMERANGE_PARAM, timerange);
        params.put(START_TIME, "'2011-06-24 04:00:00'");
        params.put(END_TIME, "'2011-06-24 05:00:00'");
        params.put(INTERVAL_PARAM, getInterval(timerange));
        params.put(TECH_PACK_RAW_TABLES, populateRawTables());
        final String query = testObj.createQuery(params, getMssTableTypes());
        assertNotNull(query);
        assert (query.length() == 0);
    }

    @Test
    public void testCreateQueryForMSCRaw() {
        params = new HashMap<String, Object>();
        final String timerange = FIFTEEN_MINUTES;
        final String type = TYPE_MSC;
        params.put(TYPE_PARAM, type);
        params.put(TIMERANGE_PARAM, timerange);
        params.put(START_TIME, "'2011-06-24 04:00:00'");
        params.put(END_TIME, "'2011-06-25 06:00:00'");
        params.put(INTERVAL_PARAM, getInterval(timerange));
        params.put(TECH_PACK_RAW_TABLES, populateRawTables());
        final String query = testObj.createQuery(params, getMssTableTypes());
        assertNotNull(query);
    }

    @Test
    public void testCreateQueryForMSCGroupRaw() {
        params = new HashMap<String, Object>();
        final String timerange = FIFTEEN_MINUTES;
        final String type = TYPE_MSC;
        params.put(TYPE_PARAM, type);
        params.put(TIMERANGE_PARAM, timerange);
        params.put(GROUP_DEFINITIONS, populateGroupDefintion());
        params.put(START_TIME, "'2011-06-24 04:00:00'");
        params.put(END_TIME, "'2011-06-25 06:00:00'");
        params.put(INTERVAL_PARAM, getInterval(timerange));
        params.put(TECH_PACK_RAW_TABLES, populateRawTables());
        final String query = testObj.createQuery(params, getMssTableTypes());
        assertNotNull(query);
    }

}
