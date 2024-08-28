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
 * @author echimma
 * @since 2011
 * 
 */
public class MssKPIRatioResourceIntegrationTest extends MssDataServiceBaseTestCase {
    private MultivaluedMap<String, String> map;

    private MssKPIRatioResource mssKPIRatioResource;

    private static final String MAX_ROWS_VALUE = "500";

    private static final String TIME_5_MINS = "5";//TR1 (RAW Tables)

    private static final String TIME_30_MINS = "30";//TR2 (1MIN --> RAW)

    private static final String TIME_3_HOURS = "360";//TR3 (15MIN --> Aggregation table)

    private static final String TIME_8_DAYS = "11520";//TR4 (1DAY --> Aggregation table)

    private static final String EVENT_ID_1 = "1";

    private static final String LOC_EVENT_ID_6 = "6";

    private static final String SMS_MS_ORIGINATING_EVENT_ID = "4";

    private static final String SMS_MS_TERMINATING_EVENT_ID = "5";

    private static final String TZ_OFFSET_0100 = "+0100";

    private static final String EVNTSRC_ID = "8347002656519852921";

    private static final String HIER3_ID = "4027908921882107646";

    private static final String HIER321_ID = "6644564130529927101";

    private static final String FAULT_CODE = "3747";

    private static final String INTERNAL_CAUSE_CODE = "6";

    private static final String REQUEST_ID = "requestID";

    private static final String DISPLAY_TYPE = GRID_PARAM;

    @Override
    public void onSetUp() {
        mssKPIRatioResource = new MssKPIRatioResource();

        attachDependenciesForMssBaseResource(mssKPIRatioResource);
        map = new MultivaluedMapImpl();
    }

    private void setMapParametersWithTimeTypeEventID(final MultivaluedMap<String, String> map, final String type,
            final String time, final String eventID) {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, type);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(EVENT_ID_PARAM, eventID);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_0100);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
    }

    private void setMapParametersWithTimeTypeEventIDHier3ID(final MultivaluedMap<String, String> map,
            final String type, final String time, final String eventID, final String hier3ID) {
        setMapParametersWithTimeTypeEventID(map, type, time, eventID);
        map.putSingle(CONTROLLER_SQL_ID, hier3ID);
    }

    private void setMapParametersWithTimeTypeEventIDEvntsrcID(final MultivaluedMap<String, String> map,
            final String type, final String time, final String eventID, final String evntsrcID) {
        setMapParametersWithTimeTypeEventID(map, type, time, eventID);
        map.putSingle(EVENT_SOURCE_SQL_ID, evntsrcID);
    }

    private void setMapParametersWithTimeTypeEventIDEvntsrcIDHier3ID(final MultivaluedMap<String, String> map,
            final String type, final String time, final String eventID, final String evntsrcID, final String hier3ID) {
        setMapParametersWithTimeTypeEventIDEvntsrcID(map, type, time, eventID, evntsrcID);
        map.putSingle(CONTROLLER_SQL_ID, hier3ID);
    }

    private void setMapParametersWithTimeTypeEventIDHier3IDHier321ID(final MultivaluedMap<String, String> map,
            final String type, final String time, final String eventID, final String hier3ID, final String hier321ID) {
        setMapParametersWithTimeTypeEventIDHier3ID(map, type, time, eventID, hier3ID);
        map.putSingle(CELL_SQL_ID, hier321ID);
    }

    private void setMapParametersWithTimeTypeEventIDEvntsrcIDHier3IDHier321ID(final MultivaluedMap<String, String> map,
            final String type, final String time, final String eventID, final String evntsrcID, final String hier3ID,
            final String hier321ID) {
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3ID(map, type, time, eventID, evntsrcID, hier3ID);
        map.putSingle(CELL_SQL_ID, hier321ID);
    }

    private void setMapParametersWithTimeTypeEventIDHier321ID(final MultivaluedMap<String, String> map,
            final String type, final String time, final String eventID, final String hier321ID) {
        // TODO Auto-generated method stub
        setMapParametersWithTimeTypeEventID(map, type, time, eventID);
        map.putSingle(CELL_SQL_ID, hier321ID);
    }

    @Test
    public void testTypeBSCRAW() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=BSC&display=grid&time=5&eventID=1&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDHier3ID(map, TYPE_BSC, TIME_5_MINS, EVENT_ID_1, HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCAgg15Min() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=BSC&display=grid&time=180&eventID=1&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDHier3ID(map, TYPE_BSC, TIME_3_HOURS, EVENT_ID_1, HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCAgg1Day() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=BSC&display=grid&time=11520&eventID=1&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDHier3ID(map, TYPE_BSC, TIME_8_DAYS, EVENT_ID_1, HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCRAW() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=MSC&display=grid&time=5&eventID=1&EVNTSRC_ID=8347002656519852921&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDEvntsrcID(map, TYPE_MSC, TIME_5_MINS, EVENT_ID_1, EVNTSRC_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCAgg15Min() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=MSC&display=grid&time=180&eventID=1&EVNTSRC_ID=8347002656519852921&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDEvntsrcID(map, TYPE_MSC, TIME_3_HOURS, EVENT_ID_1, EVNTSRC_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCAgg1Day() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=MSC&display=grid&time=11520&eventID=1&EVNTSRC_ID=8347002656519852921&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDEvntsrcID(map, TYPE_MSC, TIME_8_DAYS, EVENT_ID_1, EVNTSRC_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeBSCRAW() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=MSC&display=grid&time=5&eventID=1&EVNTSRC_ID=8347002656519852921&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3ID(map, TYPE_MSC, TIME_5_MINS, EVENT_ID_1, EVNTSRC_ID,
                HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeBSC15Min() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=MSC&display=grid&time=180&eventID=1&EVNTSRC_ID=8347002656519852921&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3ID(map, TYPE_MSC, TIME_3_HOURS, EVENT_ID_1, EVNTSRC_ID,
                HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeBSC1Day() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=MSC&display=grid&time=11520&eventID=1&EVNTSRC_ID=8347002656519852921&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3ID(map, TYPE_MSC, TIME_8_DAYS, EVENT_ID_1, EVNTSRC_ID,
                HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCDrillTypeCELLRAW() throws Exception {
        setMapParametersWithTimeTypeEventIDHier3IDHier321ID(map, TYPE_BSC, TIME_5_MINS, EVENT_ID_1, HIER3_ID,
                HIER321_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCDrillTypeCELL15Min() throws Exception {
        setMapParametersWithTimeTypeEventIDHier3IDHier321ID(map, TYPE_BSC, TIME_3_HOURS, EVENT_ID_1, HIER3_ID,
                HIER321_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCDrillTypeCELLDAY() throws Exception {
        setMapParametersWithTimeTypeEventIDHier3IDHier321ID(map, TYPE_BSC, TIME_8_DAYS, EVENT_ID_1, HIER3_ID,
                HIER321_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeCELLRAW() throws Exception {
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3IDHier321ID(map, TYPE_MSC, TIME_5_MINS, EVENT_ID_1,
                EVNTSRC_ID, HIER3_ID, HIER321_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeCELL15Min() throws Exception {
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3IDHier321ID(map, TYPE_MSC, TIME_3_HOURS, EVENT_ID_1,
                EVNTSRC_ID, HIER3_ID, HIER321_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeCELL1Day() throws Exception {
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3IDHier321ID(map, TYPE_MSC, TIME_8_DAYS, EVENT_ID_1,
                EVNTSRC_ID, HIER3_ID, HIER321_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeCELLDrillTypeCELLRAW() throws Exception {
        setMapParametersWithTimeTypeEventIDHier321ID(map, TYPE_CELL, TIME_5_MINS, EVENT_ID_1, HIER321_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeCELLDrillTypeCELL15Min() throws Exception {
        setMapParametersWithTimeTypeEventIDHier321ID(map, TYPE_CELL, TIME_3_HOURS, EVENT_ID_1, HIER321_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeCELLDrillTypeCELL1Day() throws Exception {
        setMapParametersWithTimeTypeEventIDHier321ID(map, TYPE_CELL, TIME_8_DAYS, EVENT_ID_1, HIER321_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillDownEventRAW() throws Exception {
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3IDHier321ID(map, TYPE_MSC, TIME_30_MINS, EVENT_ID_1,
                EVNTSRC_ID, HIER3_ID, HIER321_ID);
        map.putSingle(INTERNAL_CAUSE_CODE_PARAM, INTERNAL_CAUSE_CODE);
        map.putSingle(FAULT_CODE_PARAM, FAULT_CODE);
        final String result = mssKPIRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCDrillDownEventRAW() throws Exception {
        setMapParametersWithTimeTypeEventIDHier3IDHier321ID(map, TYPE_BSC, TIME_30_MINS, EVENT_ID_1, HIER3_ID,
                HIER321_ID);
        map.putSingle(INTERNAL_CAUSE_CODE_PARAM, INTERNAL_CAUSE_CODE);
        map.putSingle(FAULT_CODE_PARAM, FAULT_CODE);
        final String result = mssKPIRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeCELLDrillDownEventRAW() throws Exception {
        setMapParametersWithTimeTypeEventIDHier321ID(map, TYPE_CELL, TIME_30_MINS, EVENT_ID_1, HIER321_ID);
        map.putSingle(INTERNAL_CAUSE_CODE_PARAM, INTERNAL_CAUSE_CODE);
        map.putSingle(FAULT_CODE_PARAM, FAULT_CODE);
        final String result = mssKPIRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeMSCLocationServiceRAW() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=MSC&display=grid&time=5&eventID=1&EVNTSRC_ID=8347002656519852921&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDEvntsrcID(map, TYPE_MSC, TIME_5_MINS, LOC_EVENT_ID_6, EVNTSRC_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeMSCLocationServiceAgg15MIN() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=MSC&display=grid&time=5&eventID=1&EVNTSRC_ID=8347002656519852921&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDEvntsrcID(map, TYPE_MSC, TIME_3_HOURS, LOC_EVENT_ID_6, EVNTSRC_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeMSCLocationServiceAgg1DAY() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=MSC&display=grid&time=5&eventID=1&EVNTSRC_ID=8347002656519852921&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDEvntsrcID(map, TYPE_MSC, TIME_8_DAYS, LOC_EVENT_ID_6, EVNTSRC_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeBSCLocationServiceRAW() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=MSC&display=grid&time=5&eventID=1&EVNTSRC_ID=8347002656519852921&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3ID(map, TYPE_MSC, TIME_5_MINS, LOC_EVENT_ID_6, EVNTSRC_ID,
                HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeBSCLocationService15MIN() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=MSC&display=grid&time=5&eventID=1&EVNTSRC_ID=8347002656519852921&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3ID(map, TYPE_MSC, TIME_3_HOURS, LOC_EVENT_ID_6, EVNTSRC_ID,
                HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeBSCLocationService1DAY() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=MSC&display=grid&time=5&eventID=1&EVNTSRC_ID=8347002656519852921&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3ID(map, TYPE_MSC, TIME_8_DAYS, LOC_EVENT_ID_6, EVNTSRC_ID,
                HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeEventsLocationServiceRAW() throws Exception {
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3IDHier321ID(map, TYPE_MSC, TIME_30_MINS, LOC_EVENT_ID_6,
                EVNTSRC_ID, HIER3_ID, HIER321_ID);
        final String result = mssKPIRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeEventsmsOriginatingSMSServiceRAW() throws Exception {
        //http://atrcxb1022.athtem.eei.ericsson.se:18080/EniqEventsServices/MSS/NETWORK/KPI_RATIO?time=30&display=grid&tzOffset=+0100&maxRows=500&eventID=5&EVNTSRC_ID=8347002656519852921&HIER3_ID=5386564559998864911&HIER321_ID=174893038221952711&type=MSC
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3IDHier321ID(map, TYPE_MSC, TIME_30_MINS,
                SMS_MS_ORIGINATING_EVENT_ID, EVNTSRC_ID, HIER3_ID, HIER321_ID);
        final String result = mssKPIRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeEventsmsTerminatingSMSServiceRAW() throws Exception {
        //http://atrcxb1022.athtem.eei.ericsson.se:18080/EniqEventsServices/MSS/NETWORK/KPI_RATIO?time=30&display=grid&tzOffset=+0100&maxRows=500&eventID=5&EVNTSRC_ID=8347002656519852921&HIER3_ID=5386564559998864911&HIER321_ID=174893038221952711&type=MSC
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3IDHier321ID(map, TYPE_MSC, "120", SMS_MS_TERMINATING_EVENT_ID,
                EVNTSRC_ID, HIER3_ID, HIER321_ID);
        final String result = mssKPIRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCDrillTypeBSCLocationServiceRAW() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=BSC&display=grid&time=5&eventID=1&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDHier3ID(map, TYPE_BSC, TIME_5_MINS, LOC_EVENT_ID_6, HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCDrillTypeBSCLocationService15MIN() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=BSC&display=grid&time=5&eventID=1&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDHier3ID(map, TYPE_BSC, TIME_3_HOURS, LOC_EVENT_ID_6, HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCDrillTypeBSCLocationService1DAY() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=BSC&display=grid&time=5&eventID=1&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        setMapParametersWithTimeTypeEventIDHier3ID(map, TYPE_BSC, TIME_8_DAYS, LOC_EVENT_ID_6, HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCDrilltypeEventsLocationServiceRAW() throws Exception {
        setMapParametersWithTimeTypeEventIDHier3IDHier321ID(map, TYPE_BSC, TIME_30_MINS, LOC_EVENT_ID_6, HIER3_ID,
                HIER321_ID);
        final String result = mssKPIRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCDrilltypeEventsmsOriginatingSMSServiceRAW() throws Exception {
        setMapParametersWithTimeTypeEventIDHier3IDHier321ID(map, TYPE_BSC, TIME_30_MINS, SMS_MS_ORIGINATING_EVENT_ID,
                HIER3_ID, HIER321_ID);
        final String result = mssKPIRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCDrilltypeEventsmsTerminatingSMSServiceRAW() throws Exception {
        setMapParametersWithTimeTypeEventIDHier3IDHier321ID(map, TYPE_BSC, TIME_30_MINS, SMS_MS_TERMINATING_EVENT_ID,
                HIER3_ID, HIER321_ID);
        final String result = mssKPIRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeCELLDrilltypeEventsLocationServiceRAW() throws Exception {
        setMapParametersWithTimeTypeEventIDHier321ID(map, TYPE_CELL, TIME_30_MINS, LOC_EVENT_ID_6, HIER321_ID);
        final String result = mssKPIRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeCELLDrilltypeEventsmsOriginatingSMSRAW() throws Exception {

        setMapParametersWithTimeTypeEventIDHier321ID(map, TYPE_CELL, TIME_30_MINS, SMS_MS_ORIGINATING_EVENT_ID,
                HIER321_ID);
        final String result = mssKPIRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeCELLDrilltypeEventsmsTerminatingSMSRAW() throws Exception {

        setMapParametersWithTimeTypeEventIDHier321ID(map, TYPE_CELL, TIME_30_MINS, SMS_MS_TERMINATING_EVENT_ID,
                HIER321_ID);
        final String result = mssKPIRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeMSCSMSmsOriginatingRAW() throws Exception {
        setMapParametersWithTimeTypeEventIDEvntsrcID(map, TYPE_MSC, TIME_5_MINS, SMS_MS_ORIGINATING_EVENT_ID,
                EVNTSRC_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeMSCSMSmsOriginating15MIN() throws Exception {
        setMapParametersWithTimeTypeEventIDEvntsrcID(map, TYPE_MSC, TIME_3_HOURS, SMS_MS_ORIGINATING_EVENT_ID,
                EVNTSRC_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeMSCSMSmsOriginating1DAY() throws Exception {
        setMapParametersWithTimeTypeEventIDEvntsrcID(map, TYPE_MSC, TIME_8_DAYS, SMS_MS_ORIGINATING_EVENT_ID,
                EVNTSRC_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeMSCSMSmsTerminatingRAW() throws Exception {
        setMapParametersWithTimeTypeEventIDEvntsrcID(map, TYPE_MSC, TIME_5_MINS, SMS_MS_TERMINATING_EVENT_ID,
                EVNTSRC_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeMSCSMSmsTerminating15MIN() throws Exception {
        setMapParametersWithTimeTypeEventIDEvntsrcID(map, TYPE_MSC, TIME_3_HOURS, SMS_MS_TERMINATING_EVENT_ID,
                EVNTSRC_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeMSCSMSmsTerminating1DAY() throws Exception {
        setMapParametersWithTimeTypeEventIDEvntsrcID(map, TYPE_MSC, TIME_8_DAYS, SMS_MS_TERMINATING_EVENT_ID,
                EVNTSRC_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeBSCSMSOriginatingRAW() throws Exception {
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3ID(map, TYPE_MSC, TIME_5_MINS, SMS_MS_ORIGINATING_EVENT_ID,
                EVNTSRC_ID, HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeBSCSMSOriginating15MIN() throws Exception {
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3ID(map, TYPE_MSC, TIME_3_HOURS, SMS_MS_ORIGINATING_EVENT_ID,
                EVNTSRC_ID, HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeMSCDrillTypeBSCSMSOriginating1DAY() throws Exception {
        setMapParametersWithTimeTypeEventIDEvntsrcIDHier3ID(map, TYPE_MSC, TIME_8_DAYS, SMS_MS_ORIGINATING_EVENT_ID,
                EVNTSRC_ID, HIER3_ID);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testErrorEmptyRAWTables() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TYPE_PARAM, TYPE_MSC);

        map.putSingle(TIME_FROM_QUERY_PARAM, "0000");
        map.putSingle(TIME_TO_QUERY_PARAM, "0030");
        map.putSingle(DATA_TIME_FROM_QUERY_PARAM, "01062001");
        map.putSingle(DATA_TIME_TO_QUERY_PARAM, "01062001");
        map.putSingle(TZ_OFFSET, "+0100");

        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(EVENT_SOURCE_SQL_ID, EVNTSRC_ID);
        map.putSingle(EVENT_ID_PARAM, EVENT_ID_1);
        final String result = mssKPIRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testErrorParameterNoGird() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?type=MSC&time=30&eventID=1&EVNTSRC_ID=8347002656519852921&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_30_MINS);
        map.putSingle(EVENT_ID_PARAM, EVENT_ID_1);
        map.putSingle(EVENT_SOURCE_SQL_ID, EVNTSRC_ID);
        map.putSingle(CONTROLLER_SQL_ID, HIER3_ID);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_0100);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONErrorResult(result);
    }

    @Test
    public void testErrorParameterNoType() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?display=grid&time=30&eventID=1&EVNTSRC_ID=8347002656519852921&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_30_MINS);
        map.putSingle(EVENT_ID_PARAM, EVENT_ID_1);
        map.putSingle(EVENT_SOURCE_SQL_ID, EVNTSRC_ID);
        map.putSingle(CONTROLLER_SQL_ID, HIER3_ID);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_0100);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertJSONErrorResult(result);
    }

    @Test
    public void testErrorDisplayType() throws Exception {
        ///EniqEventsServices/MSS/NETWORK/KPI_RATIO?display=errorDisplayType&time=30&eventID=1&EVNTSRC_ID=8347002656519852921&HIER3_ID=1487308146838068860&tzOffset=+0100&maxRows=500
        map.clear();
        final String errorDisplayType = "errorDisplayType";
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(DISPLAY_PARAM, errorDisplayType);
        map.putSingle(TIME_QUERY_PARAM, TIME_30_MINS);
        map.putSingle(EVENT_ID_PARAM, EVENT_ID_1);
        map.putSingle(EVENT_SOURCE_SQL_ID, EVNTSRC_ID);
        map.putSingle(CONTROLLER_SQL_ID, HIER3_ID);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_0100);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = mssKPIRatioResource.getData(REQUEST_ID, map);
        assertResultContains(result, errorDisplayType);
    }
}