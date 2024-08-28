/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.mss.piechart;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.integritytests.mss.piechart.MSSPieChartCauseCodePopulator.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.integritytests.mss.MssTestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.resources.mss.piechart.MSSCauseCodeAnalysisPieChartResource;
import com.ericsson.eniq.events.server.test.queryresults.mss.MSSPieChartSubCauseCodeAnalysisResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MSSPieChartSubCauseCodeAnalysisWithPreparedTablesTest extends
        MssTestsWithTemporaryTablesBaseTestCase<MSSPieChartSubCauseCodeAnalysisResult> {

    private final MSSCauseCodeAnalysisPieChartResource mssCauseCodeAnalysisPieChartResource = new MSSCauseCodeAnalysisPieChartResource();

    private final MSSPieChartCauseCodeDataValidator mssPieChartCauseCodeDataValidator = new MSSPieChartCauseCodeDataValidator();

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        attachDependenciesForMSSBaseResource(mssCauseCodeAnalysisPieChartResource);
        final MSSPieChartCauseCodePopulator pieChartCauseCodePopulator = new MSSPieChartCauseCodePopulator(
                this.connection);
        pieChartCauseCodePopulator.createTemporaryTables();
        pieChartCauseCodePopulator.populateTemporaryTables();
    }

    @Test
    public void testGetSubCauseCodeAnalysisByBSC() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(NODE_PARAM, TEST_VALUE_BSC);
        map.putSingle(INTERNAL_CAUSE_CODE_PARAM, "" + internal_causeCode_1);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);

        final List<MSSPieChartSubCauseCodeAnalysisResult> result = getResults(MSS + UNDERSCORE + CAUSE_CODE_PIE_CHART
                + PATH_SEPARATOR + SUB_CAUSE_CODE_ANALYSIS);
        mssPieChartCauseCodeDataValidator.makeAssertionsSCCAnaylsis(result);
    }

    @Test
    public void testGetSubCauseCodeAnalysisByMSC() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_MSC);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(NODE_PARAM, TEST_VALUE_MSC);
        map.putSingle(INTERNAL_CAUSE_CODE_PARAM, "" + internal_causeCode_1);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);

        final List<MSSPieChartSubCauseCodeAnalysisResult> result = getResults(MSS + UNDERSCORE + CAUSE_CODE_PIE_CHART
                + PATH_SEPARATOR + SUB_CAUSE_CODE_ANALYSIS);
        mssPieChartCauseCodeDataValidator.makeAssertionsSCCAnaylsis(result);
    }

    @Test
    public void testGetSubCauseCodeAnalysisByCell() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(NODE_PARAM, TEST_VALUE_CELL);
        map.putSingle(INTERNAL_CAUSE_CODE_PARAM, "" + internal_causeCode_1);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        DummyUriInfoImpl.setUriInfoForMss(map, mssCauseCodeAnalysisPieChartResource);

        final List<MSSPieChartSubCauseCodeAnalysisResult> result = getResults(MSS + UNDERSCORE + CAUSE_CODE_PIE_CHART
                + PATH_SEPARATOR + SUB_CAUSE_CODE_ANALYSIS);
        mssPieChartCauseCodeDataValidator.makeAssertionsSCCAnaylsis(result);
    }

    private List<MSSPieChartSubCauseCodeAnalysisResult> getResults(final String path) throws Exception {
        final String json = mssCauseCodeAnalysisPieChartResource.getResults(path);
        System.out.println(json);
        return getTranslator().translateResult(json, MSSPieChartSubCauseCodeAnalysisResult.class);
    }
}