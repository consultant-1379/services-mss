package com.ericsson.eniq.events.server.resources.mss;

import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.mss.piechart.MSSCauseCodeAnalysisPieChartAPI;
import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;

/**
 * 
 * @author echimma
 * @since 2011
 *
 */
public class MssNetworkServiceResourceMockedTest extends BaseJMockUnitTest {
    private MssNetworkServiceResource mssNetworkServiceResource;

    @Before
    public void setUp() {

        mssNetworkServiceResource = new MssNetworkServiceResource();

    }

    @Test
    public void testGetMssEventAnalysisResource() {
        final MssEventAnalysisResource mockMssEventAnalysisResource = mockery.mock(MssEventAnalysisResource.class);
        mssNetworkServiceResource.mssEventAnalysisResource = mockMssEventAnalysisResource;
        assertNotNull(mssNetworkServiceResource.getMssEventAnalysisResource());
    }

    @Test
    public void testGetMssMultipleRankingResource() {
        final MssMultipleRankingResource mockMssMultipleRankingResource = mockery
                .mock(MssMultipleRankingResource.class);
        mssNetworkServiceResource.mssMultipleRankingResource = mockMssMultipleRankingResource;
        assertNotNull(mssNetworkServiceResource.getMssMultipleRankingResource());
    }

    @Test
    public void testGetMssCauseCodeAnalysisResource() {
        final MssCauseCodeAnalysisResource mockMssCauseCodeAnalysisResource = mockery
                .mock(MssCauseCodeAnalysisResource.class);
        mssNetworkServiceResource.mssCauseCodeAnalysisResource = mockMssCauseCodeAnalysisResource;
        assertNotNull(mssNetworkServiceResource.getMssCauseCodeAnalysisResource());
    }

    @Test
    public void testGetKPIResource() {
        final MssKPIResource mockMssKPIResource = mockery.mock(MssKPIResource.class);
        mssNetworkServiceResource.kpi = mockMssKPIResource;
        assertNotNull(mssNetworkServiceResource.getKPIResource());
    }

    @Test
    public void testGetMssKPIRatioResource() {
        final MssKPIRatioResource mockMssKPIRatioResource = mockery.mock(MssKPIRatioResource.class);
        mssNetworkServiceResource.mssKPIRatioResource = mockMssKPIRatioResource;
        assertNotNull(mssNetworkServiceResource.getMssKPIRatioResource());
    }

    @Test
    public void testGetMssRoamingAnalysis() {
        final MssRoamingAnalysisResource mockMssRoamingAnalysisResource = mockery
                .mock(MssRoamingAnalysisResource.class);
        mssNetworkServiceResource.mssRoamingAnalysisResource = mockMssRoamingAnalysisResource;
        assertNotNull(mssNetworkServiceResource.getMssRoamingAnalysis());
    }

    @Test
    public void testGetMssEventVolume() {
        final MssEventVolumeResource mockMssEventVolumeResource = mockery.mock(MssEventVolumeResource.class);
        mssNetworkServiceResource.mssEventVolumeResource = mockMssEventVolumeResource;
        assertNotNull(mssNetworkServiceResource.getMssEventVolume());
    }

    @Test
    public void testGetMssCauseCodeAnalysisPieChartAPI() {
        final MSSCauseCodeAnalysisPieChartAPI mockMSSCauseCodeAnalysisPieChartAPI = mockery
                .mock(MSSCauseCodeAnalysisPieChartAPI.class);
        mssNetworkServiceResource.mssCauseCodeAnalysisPieChartAPI = mockMSSCauseCodeAnalysisPieChartAPI;
        assertNotNull(mssNetworkServiceResource.getMssCauseCodeAnalysisPieChartAPI());
    }
}
