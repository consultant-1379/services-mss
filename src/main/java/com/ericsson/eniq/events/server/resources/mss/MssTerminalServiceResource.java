/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.Path;

import com.ericsson.eniq.events.server.resources.GroupMgtResource;

/**
 * The Class Mss TerminalServiceResource.
 * Sub-root resource of RESTful service.
 * 
 * @author echchik
 * 
 * @since  May 2011
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class MssTerminalServiceResource {

    @EJB
    protected MssEventAnalysisResource mssEventAnalysisResource;

    @EJB
    protected MssKPIResource mssKPIResource;

    @EJB
    protected MssMultipleRankingResource mssMultipleRankingResource;

    @EJB
    protected GroupMgtResource groupMgtResource;

    /**
     * Gets the event analysis resource.
     *
     * @return the event analysis resource
     */
    @Path(EVENT_ANALYSIS)
    public MssEventAnalysisResource getMssEventAnalysisResource() {
        return this.mssEventAnalysisResource;
    }

    /**
     * Gets the multiple ranking resource.
     *
     * @return the multiple ranking resource
     */
    @Path(RANKING_ANALYSIS)
    public MssMultipleRankingResource getMssMultipleRankingResource() {
        return this.mssMultipleRankingResource;
    }

    /**
     * Gets the kpi resource.
     *
     * @return the kpi resource
     */
    @Path(KPI)
    public MssKPIResource getMssKpiResource() {
        return this.mssKPIResource;
    }

    /**
     * Gets the group management resource.
     *
     * @return the event analysis resource
     */
    @Path(GROUP)
    public GroupMgtResource getGroupMgtResource() {
        return groupMgtResource;
    }
}