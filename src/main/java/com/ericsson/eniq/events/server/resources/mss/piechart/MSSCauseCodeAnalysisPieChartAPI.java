/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss.piechart;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import com.ericsson.eniq.events.server.common.MediaTypeConstants;
import com.ericsson.eniq.events.server.json.JSONException;

/**
 * This is the API for MSS cause code pie chart analysis
 * The idea is to hide the actual resource from external world
 * This implementation will be revisited when the services framework will be in place
 * 
 * @author eavidat
 * @since 2011
 *
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class MSSCauseCodeAnalysisPieChartAPI {

    @EJB
    protected MSSCauseCodeAnalysisPieChartResource mssCauseCodeAnalysisPieChartResource;

    /**
     * This API returns a list of internal cause codes 
     * for a particular node type (single or group) and 
     * for a specific time period
     * 
     * @return the list cause codes
     * with the following JSON structure:  
     *      {
                   "1" : "Internal Cause Code ID",
                   "2" : "Internal Cause Code DESC" 
            }
     *  
     * @throws WebApplicationException
     * @throws JSONException
     */
    @Path(CC_LIST)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getCausecodeList() throws WebApplicationException, JSONException {
        return this.mssCauseCodeAnalysisPieChartResource.getResults(MSS + UNDERSCORE + CAUSE_CODE_PIE_CHART
                + PATH_SEPARATOR + CC_LIST);
    }

    /**
     * This API returns the internal cause code analysis data (e.g. number of errors, impacted subscribers)
     * for the selected internal cause code (s) and
     * for a particular node type (single or group) and 
     * for a specific time period
     * 
     * @return the cause code analysis
     * with the following JSON structure:  
     *      {
                   "1" : "Internal Cause Code ID",
                   "2" : "Internal Cause Code DESC", 
                   "3" : "Number of failures",
                   "4" : "Impacted Subscribers"
            }
     *   
     * @throws WebApplicationException
     * @throws JSONException
     */
    @Path(CAUSE_CODE_ANALYSIS)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getCauseCodeAnalysis() throws WebApplicationException, JSONException {
        return this.mssCauseCodeAnalysisPieChartResource.getResults(MSS + UNDERSCORE + CAUSE_CODE_PIE_CHART
                + PATH_SEPARATOR + CAUSE_CODE_ANALYSIS);
    }

    /**
     * This API returns the fault code analysis data (e.g. number of errors, impacted subscribers, what next advice)
     * for a selected internal cause code and
     * for a particular node type (single or group) and 
     * for a specific time period
     * 
     * @return the sub cause code analysis
     * with the following JSON structure:  
     *      {
                   "1" : "Internal Cause Code ID",
                   "2" : "Fault Code ID",
                   "3" : "Fault Code DESC",
                   "4" : "What Next Text",
                   "5" : "Number of failures",
                   "6" : "Impacted Subscribers"
            }
     *   
     * @throws WebApplicationException
     * @throws JSONException
     */
    @Path(SUB_CAUSE_CODE_ANALYSIS)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getSubCauseCodeAnalysis() throws WebApplicationException, JSONException {
        return this.mssCauseCodeAnalysisPieChartResource.getResults(MSS + UNDERSCORE + CAUSE_CODE_PIE_CHART
                + PATH_SEPARATOR + SUB_CAUSE_CODE_ANALYSIS);
    }
}