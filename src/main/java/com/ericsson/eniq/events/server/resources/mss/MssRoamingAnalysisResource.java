/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.queryconstructor.QueryConstructor;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * The Class RoamingAnalysisResource.
 *
 * @author ezhibhe
 * @since  Jun 2011
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class MssRoamingAnalysisResource extends MssBaseResource {

    @EJB
    protected QueryConstructor queryConstructor;

    static final List<String> listOfTechPacks;

    static {
        listOfTechPacks = new ArrayList<String>();
        listOfTechPacks.add(EVENT_E_MSS_TPNAME);
    }

    /**
     * @param queryConstructor the queryConstructor to set
     */
    public void setQueryConstructor(final QueryConstructor queryConstructor) {
        this.queryConstructor = queryConstructor;
    }

    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        throw new UnsupportedOperationException();
    }

    @Path(ROAMING_COUNTRY)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getRoamingCountryData() throws WebApplicationException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRoamingResults(requestId, TYPE_ROAMING_COUNTRY, getDecodedQueryParameters());
    }

    /**
     * Roaming by operator.
     * 
     * @return JSON encoded results
     * @throws WebApplicationException
     */
    @Path(ROAMING_OPERATOR)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getRoamingOperatorData() throws WebApplicationException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRoamingResults(requestId, TYPE_ROAMING_OPERATOR, getDecodedQueryParameters());
    }

    /**
     * Get Roaming Results:
     * (Both grid and chart display types as user can for example 
     * change time when in grid version of chart - so valid that "grid" can be in call
     * the UI can handle chart style data in the grid).
     * 
     * @param requestId corresponds to this request for cancelling later
     * @param roamingObject - roaming object [county,operator]
     * @param requestParameters - URL query parameters
     * @return JSON encoded string
     * @throws WebApplicationException
     */
    public String getRoamingResults(final String requestId, final String roamingObject,
            final MultivaluedMap<String, String> requestParameters) throws WebApplicationException {

        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(CHART_PARAM) || displayType.equals(GRID_PARAM)) {
            return getChartResults(requestId, roamingObject, requestParameters);
        }
        return getNoSuchDisplayErrorResponse(displayType);
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#checkParameters(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected List<String> checkParameters(final MultivaluedMap<String, String> requestParameters) {
        final List<String> errors = new ArrayList<String>();

        if (!requestParameters.containsKey(DISPLAY_PARAM)) {
            errors.add(DISPLAY_PARAM);
        }
        return errors;
    }

    /**
     * Gets the JSON chart results for network roaming.
     *
     * @param requestId corresponds to this request for cancelling later
     * @param requestParameters - URL query parameters
     * @param dateTimeRange - formatted date time range
     * @return JSON encoded results
     * @throws WebApplicationException
     */
    private String getChartResults(final String requestId, final String roamingObject,
            final MultivaluedMap<String, String> requestParameters) throws WebApplicationException {
        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(
                requestParameters, listOfTechPacks);
        final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();
        final Map<String, Object> templateParameters = new HashMap<String, Object>();
        // timerange
        templateParameters.put(TIMERANGE_PARAM, timerange);
        // roaming_object -- MCC or MCC & MNC
        templateParameters.put(ROAMING_OBJECT, roamingObject);

        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);

        // TECH_PACK_TABLES is used to store raw tables
        final TechPackTables techPackRawTables = getRawTables(null, dateTimeRange, listOfTechPacks);
        templateParameters.put(TECH_PACK_TABLES, techPackRawTables);
        if (techPackRawTables.shouldReportErrorAboutEmptyRawTables()) {
            return JSONUtils.JSONEmptySuccessResult();
        }

        final List<String> queries = queryConstructor.getMssRoamingAnalysisQuery(templateParameters);
        if (queries == null || queries.isEmpty()) {
            return JSONUtils.JSONBuildFailureError();
        }
        final FormattedDateTimeRange newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange,
                listOfTechPacks);
        checkAndCreateFineAuditLogEntryForQuery(requestParameters, queries, newDateTimeRange);

        return this.dataService.getChartDataWithAppendedRowsRoaming(requestId, queries,
                this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), ROAMING_X_AXIS_VALUE, "5",
                tzOffset, getLoadBalancingPolicy(requestParameters));
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        throw new UnsupportedOperationException();
    }
}
