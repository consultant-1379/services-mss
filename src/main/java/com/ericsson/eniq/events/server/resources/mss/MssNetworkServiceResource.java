/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.Path;

import com.ericsson.eniq.events.server.resources.mss.piechart.MSSCauseCodeAnalysisPieChartAPI;

/**
 * The Class MssNetworkServiceResource. Sub-root resource of RESTful service.
 * 
 * @author egraman
 * @since 2011
 * 
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class MssNetworkServiceResource {

    @EJB
    protected MssEventAnalysisResource mssEventAnalysisResource;

    @EJB
    protected MssMultipleRankingResource mssMultipleRankingResource;

    @EJB
    protected MssCauseCodeAnalysisResource mssCauseCodeAnalysisResource;

    @EJB
    protected MssKPIResource kpi;

    @EJB
    protected MssKPIRatioResource mssKPIRatioResource;

    @EJB
    protected MssRoamingAnalysisResource mssRoamingAnalysisResource;

    @EJB
    protected MssEventVolumeResource mssEventVolumeResource;

    @EJB
    protected MSSCauseCodeAnalysisPieChartAPI mssCauseCodeAnalysisPieChartAPI;

    /**
     * @return the mssEventAnalysisResource
     */
    @Path(EVENT_ANALYSIS)
    public MssEventAnalysisResource getMssEventAnalysisResource() {
        return mssEventAnalysisResource;
    }

    /**
     * @return the mssMultipleRankingResource
     */
    @Path(RANKING_ANALYSIS)
    public MssMultipleRankingResource getMssMultipleRankingResource() {
        return mssMultipleRankingResource;
    }

    /**
     * @return the mssCauseCodeAnalysisResource
     */
    @Path(INTERNAL_CAUSE_CODE_ANALYSIS)
    public MssCauseCodeAnalysisResource getMssCauseCodeAnalysisResource() {
        return mssCauseCodeAnalysisResource;
    }

    /**
     * @return the KPIResource
     */
    @Path(KPI)
    public MssKPIResource getKPIResource() {
        return kpi;
    }

    /**
     * @return the mssKPIRatioResource
     */
    @Path(KPI_RATIO)
    public MssKPIRatioResource getMssKPIRatioResource() {
        return mssKPIRatioResource;
    }

    /**
     * @return the mssRoamingAnalysisResource
     */
    @Path(ROAMING_ANALYSIS)
    public MssRoamingAnalysisResource getMssRoamingAnalysis() {
        return mssRoamingAnalysisResource;
    }

    /**
     * @return the mssEventVolumeResource
     */
    @Path(EVENT_VOLUME)
    public MssEventVolumeResource getMssEventVolume() {
        return mssEventVolumeResource;
    }

    /**
     * @return the mssCauseCodeAnalysisPieChartAPI
     */
    @Path(CAUSE_CODE_PIE_CHART)
    public MSSCauseCodeAnalysisPieChartAPI getMssCauseCodeAnalysisPieChartAPI() {
        return mssCauseCodeAnalysisPieChartAPI;
    }
}
