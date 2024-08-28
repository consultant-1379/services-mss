/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss.piechart;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.mss.MssDataServiceBaseTestCase;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * This test class is designed
 * 
 * for  all possible parameters 
 *      and all methods 
 *      and all possible paths (both java code and velocity templates)
 * 
 * of   #MSSCauseCodeAnalysisPieChartAPI 
 *      and #MSSCauseCodeAnalysisPieChartResource
 * 
 * @author eavidat
 * @since 2011
 *
 */
public class MSSCauseCodeAnalysisPieChartIntegrationTest extends MssDataServiceBaseTestCase {

    private MultivaluedMap<String, String> map;

    private MSSCauseCodeAnalysisPieChartResource mssCauseCodeAnalysisPieChartResource;

    private MSSCauseCodeAnalysisPieChartAPI mssCauseCodeAnalysisPieChartAPI;

    @Override
    public void onSetUp() {
        this.mssCauseCodeAnalysisPieChartResource = new MSSCauseCodeAnalysisPieChartResource();
        this.mssCauseCodeAnalysisPieChartAPI = new MSSCauseCodeAnalysisPieChartAPI();
        map = new MultivaluedMapImpl();
        attachDependenciesForMssBaseResource(mssCauseCodeAnalysisPieChartResource);
        this.mssCauseCodeAnalysisPieChartAPI.mssCauseCodeAnalysisPieChartResource = this.mssCauseCodeAnalysisPieChartResource;
    }

    /**
     * This method tests the cause code list API {@see MSSCauseCodeAnalysisPieChartAPI#getCausecodeList()}
     * for all time combinations and 
     * for an BSC type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetCauseCodeListByBsc() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_BSC);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(NODE_PARAM, TEST_VALUE_BSC);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getCausecodeList());
        }
    }

    /**
     * This method tests the cause code list API {@see MSSCauseCodeAnalysisPieChartAPI#getCausecodeList()}
     * for all time combinations and 
     * for an BSC group type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetCauseCodeListByBscGroup() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_BSC);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_BSC_GROUP);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getCausecodeList());
        }
    }

    /**
     * This method tests the cause code list API {@see MSSCauseCodeAnalysisPieChartAPI#getCausecodeList()}
     * for all time combinations and 
     * for an CELL type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetCauseCodeListByCell() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_CELL);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(NODE_PARAM, TEST_VALUE_CELL);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getCausecodeList());
        }
    }

    /**
     * This method tests the cause code list API {@see MSSCauseCodeAnalysisPieChartAPI#getCausecodeList()}
     * for all time combinations and 
     * for an CELL group type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetCauseCodeListByCellGroup() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_CELL);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_CELL_GROUP);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getCausecodeList());
        }
    }

    /**
     * This method tests the cause code list API {@see MSSCauseCodeAnalysisPieChartAPI#getCausecodeList()}
     * for all time combinations and 
     * for an MSC type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetCauseCodeListByMsc() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_MSC);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(NODE_PARAM, TEST_VALUE_MSC);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getCausecodeList());
        }
    }

    /**
     * This method tests the cause code list API {@see MSSCauseCodeAnalysisPieChartAPI#getCausecodeList()}
     * for all time combinations and 
     * for an MSC group type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetCauseCodeListByMscGroup() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_MSC);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSC_GROUP);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getCausecodeList());
        }
    }

    /**
     * This method tests the cause code analysis API {@see MSSCauseCodeAnalysisPieChartAPI#getCauseCodeAnalysis()}
     * for all time combinations and 
     * for a BSC type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetCauseCodeAnalysisByBsc() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_BSC);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(NODE_PARAM, TEST_VALUE_BSC);
            map.putSingle(CAUSE_CODE_IDS_PARAM, TEST_VALUE_CAUSE_CODE_IDS);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getCauseCodeAnalysis());
        }
    }

    /**
     * This method tests the cause code analysis API {@see MSSCauseCodeAnalysisPieChartAPI#getCauseCodeAnalysis()}
     * for all time combinations and 
     * for a BSC Group type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetCauseCodeAnalysisByBscGroup() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_BSC);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_BSC_GROUP);
            map.putSingle(CAUSE_CODE_IDS_PARAM, TEST_VALUE_CAUSE_CODE_IDS);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getCauseCodeAnalysis());
        }
    }

    /**
     * This method tests the cause code analysis API {@see MSSCauseCodeAnalysisPieChartAPI#getCauseCodeAnalysis()}
     * for all time combinations and 
     * for an BSC type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetCauseCodeAnalysisByMsc() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_MSC);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(NODE_PARAM, TEST_VALUE_MSC);
            map.putSingle(CAUSE_CODE_IDS_PARAM, TEST_VALUE_CAUSE_CODE_IDS);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getCauseCodeAnalysis());
        }
    }

    /**
     * This method tests the cause code analysis API {@see MSSCauseCodeAnalysisPieChartAPI#getCauseCodeAnalysis()}
     * for all time combinations and 
     * for a MSC Group type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetCauseCodeAnalysisByMscGroup() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_MSC);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSC_GROUP);
            map.putSingle(CAUSE_CODE_IDS_PARAM, TEST_VALUE_CAUSE_CODE_IDS);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getCauseCodeAnalysis());
        }
    }

    /**
     * This method tests the cause code analysis API {@see MSSCauseCodeAnalysisPieChartAPI#getCauseCodeAnalysis()}
     * for all time combinations and 
     * for an BSC type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetCauseCodeAnalysisByCell() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_CELL);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(NODE_PARAM, TEST_VALUE_CELL);
            map.putSingle(CAUSE_CODE_IDS_PARAM, TEST_VALUE_CAUSE_CODE_IDS);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getCauseCodeAnalysis());
        }
    }

    /**
     * This method tests the cause code analysis API {@see MSSCauseCodeAnalysisPieChartAPI#getCauseCodeAnalysis()}
     * for all time combinations and 
     * for a MSC Group type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetCauseCodeAnalysisByCellGroup() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_CELL);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_CELL_GROUP);
            map.putSingle(CAUSE_CODE_IDS_PARAM, TEST_VALUE_CAUSE_CODE_IDS);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getCauseCodeAnalysis());
        }
    }

    /**
     * This method tests the sub cause code analysis API {@see MSSCauseCodeAnalysisPieChartAPI#getSubCauseCodeAnalysis()}
     * for all time combinations and 
     * for a BSC type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetSubCauseCodeAnalysisByBsc() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_BSC);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(NODE_PARAM, TEST_VALUE_BSC);
            map.putSingle(INTERNAL_CAUSE_CODE_PARAM, TEST_VALUE_CAUSE_CODE);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getSubCauseCodeAnalysis());
        }
    }

    /**
     * This method tests the sub cause code analysis API {@see MSSCauseCodeAnalysisPieChartAPI#getSubCauseCodeAnalysis()}
     * for all time combinations and 
     * for a BSC Group type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetSubCauseCodeAnalysisByBscGroup() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_BSC);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_BSC_GROUP);
            map.putSingle(INTERNAL_CAUSE_CODE_PARAM, TEST_VALUE_CAUSE_CODE);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getSubCauseCodeAnalysis());
        }
    }

    /**
     * This method tests the sub cause code analysis API {@see MSSCauseCodeAnalysisPieChartAPI#getSubCauseCodeAnalysis()}
     * for all time combinations and 
     * for a MSC type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetSubCauseCodeAnalysisByMsc() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_MSC);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(NODE_PARAM, TEST_VALUE_MSC);
            map.putSingle(INTERNAL_CAUSE_CODE_PARAM, TEST_VALUE_CAUSE_CODE);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getSubCauseCodeAnalysis());
        }
    }

    /**
     * This method tests the sub cause code analysis API {@see MSSCauseCodeAnalysisPieChartAPI#getSubCauseCodeAnalysis()}
     * for all time combinations and 
     * for a MSC Group type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetSubCauseCodeAnalysisByMscGroup() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_MSC);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_MSC_GROUP);
            map.putSingle(INTERNAL_CAUSE_CODE_PARAM, TEST_VALUE_CAUSE_CODE);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getSubCauseCodeAnalysis());
        }
    }

    /**
     * This method tests the sub cause code analysis API {@see MSSCauseCodeAnalysisPieChartAPI#getSubCauseCodeAnalysis()}
     * for all time combinations and 
     * for a CELL type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetSubCauseCodeAnalysisByCell() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_CELL);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(NODE_PARAM, TEST_VALUE_CELL);
            map.putSingle(INTERNAL_CAUSE_CODE_PARAM, TEST_VALUE_CAUSE_CODE);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getSubCauseCodeAnalysis());
        }
    }

    /**
     * This method tests the sub cause code analysis API {@see MSSCauseCodeAnalysisPieChartAPI#getSubCauseCodeAnalysis()}
     * for all time combinations and 
     * for a CELL Group type 
     * 
     * @throws Exception
     */
    @Test
    public void testGetSubCauseCodeAnalysisByCellGroup() throws Exception {
        for (final String time : TIME_INPUTS_GRID_VIEW) {
            map.clear();
            map.putSingle(TYPE_PARAM, TYPE_CELL);
            map.putSingle(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET);
            map.putSingle(GROUP_NAME_PARAM, TEST_VALUE_CELL_GROUP);
            map.putSingle(INTERNAL_CAUSE_CODE_PARAM, TEST_VALUE_CAUSE_CODE);
            map.putSingle(TIME_QUERY_PARAM, time);

            DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);
            assertJSONSucceeds(mssCauseCodeAnalysisPieChartAPI.getSubCauseCodeAnalysis());
        }
    }

    /**
     * This method tests the API #MSSCauseCodeAnalysisPieChartAPI with type missing 
     * 
     * @throws Exception
     */
    @Test
    public void testTypeAbsence() throws Exception {
        map.clear();
        DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);

        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getCausecodeList());
        String testString = mssCauseCodeAnalysisPieChartAPI.getCausecodeList();
        assertResultContains(testString, TYPE_PARAM);

        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getCauseCodeAnalysis());
        testString = mssCauseCodeAnalysisPieChartAPI.getCauseCodeAnalysis();
        assertResultContains(testString, TYPE_PARAM);

        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getSubCauseCodeAnalysis());
        testString = mssCauseCodeAnalysisPieChartAPI.getSubCauseCodeAnalysis();
        assertResultContains(testString, TYPE_PARAM);
    }

    /**
     * This method tests the API #MSSCauseCodeAnalysisPieChartAPI with invalid type 
     * 
     * @throws Exception
     */
    @Test
    public void testTypeValidity() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);

        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getCausecodeList());
        String testString = mssCauseCodeAnalysisPieChartAPI.getCausecodeList();
        assertResultContains(testString, TYPE_PARAM);

        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getCauseCodeAnalysis());
        testString = mssCauseCodeAnalysisPieChartAPI.getCauseCodeAnalysis();
        assertResultContains(testString, TYPE_PARAM);

        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getSubCauseCodeAnalysis());
        testString = mssCauseCodeAnalysisPieChartAPI.getSubCauseCodeAnalysis();
        assertResultContains(testString, TYPE_PARAM);
    }

    /**
     * This method tests the API #MSSCauseCodeAnalysisPieChartAPI with invalid BSC 
     * 
     * @throws Exception
     */
    @Test
    public void testBSCValidity() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(NODE_PARAM, "+" + TEST_VALUE_BSC);
        DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);

        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getCausecodeList());
        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getCauseCodeAnalysis());
        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getSubCauseCodeAnalysis());
    }

    /**
     * This method tests the API #MSSCauseCodeAnalysisPieChartAPI with invalid CELL 
     * 
     * @throws Exception
     */
    @Test
    public void testCELLValidity() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(NODE_PARAM, "+" + TEST_VALUE_CELL);
        DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);

        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getCausecodeList());
        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getCauseCodeAnalysis());
        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getSubCauseCodeAnalysis());
    }

    /**
     * This method tests the API #MSSCauseCodeAnalysisPieChartAPI with invalid group 
     * 
     * @throws Exception
     */
    @Test
    public void testGroupNameValidity() throws Exception {
        map.clear();
        map.putSingle(GROUP_NAME_PARAM, "+" + TEST_VALUE_APN);
        DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);

        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getCausecodeList());
        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getCauseCodeAnalysis());
        assertJSONErrorResult(mssCauseCodeAnalysisPieChartAPI.getSubCauseCodeAnalysis());
    }
}