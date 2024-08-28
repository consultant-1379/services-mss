/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.*;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.common.ApplicationConstants;
import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.MediaTypeConstants;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.common.tablesandviews.TableType;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPack;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * The Class MultipleRankingResource. This service returns MSS ranking data for
 * the UI widgets from Sybase IQ. The actual data comes from a normal JSON data
 * source.
 * 
 * @author egraman
 * @author echimma
 * @author ezhibhe
 * @since 2011
 * 
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class MssMultipleRankingResource extends MssBaseResource {

    public MssMultipleRankingResource() {
        aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_BSC, new AggregationTableInfo(VEND_HIER3, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_RNC, new AggregationTableInfo(VEND_HIER3, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_CELL, new AggregationTableInfo(VEND_HIER321, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_INTERNAL_CAUSE_CODE, new AggregationTableInfo(EVNTSRC_CC,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_TAC, new AggregationTableInfo("MANUF_TAC", EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_MSC, new AggregationTableInfo(EVNTSRC, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_MSS_IMSI_LONG_DURATION_CALLS, new AggregationTableInfo(IMSI_LONG_DUR_RANK,
                EventDataSourceType.AGGREGATED_15MIN));
        aggregationViews.put(TYPE_MSS_IMSI_SHORT_DURATION_CALLS, new AggregationTableInfo(IMSI_SHORT_DUR_RANK,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_MSS_MS_ORIGINATING_UNANSWERED, new AggregationTableInfo(IMSI_UNANSWERED_CALL_RANK,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_MSS_MS_TERMINATING_UNANSWERED, new AggregationTableInfo(IMSI_UNANSWERED_CALL_RANK,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
    }

    /**
     * Gets the Voice Dropped Calls Ranking
     *
     * @return the Voice Dropped Calls Ranking
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path("/{callType}/{errorType}")
    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    public String getData(@PathParam("callType")
    final String callType, @PathParam("errorType")
    final String errorType) throws WebApplicationException {
        if (!VOICE.equals(callType)) {
            throw new UnsupportedOperationException(String.format("There is no resource %s/%s", RANKING_ANALYSIS,
                    callType));
        }
        if (!MssMultipleErrorType.checkValue(errorType)) {
            throw new UnsupportedOperationException(String.format("There is no resource %s/%s/%s", RANKING_ANALYSIS,
                    callType, errorType));
        }

        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, getDecodedQueryParameters(), callType, errorType);
    }

    /**
     * Gets the Voice Dropped Calls Ranking as CSV file
     *
     * @return the Voice Dropped Calls Ranking Response
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path("/{callType}/{errorType}")
    @GET
    @Produces({ MediaTypeConstants.APPLICATION_CSV })
    public Response getDataAsCSV(@PathParam("callType")
    final String callType, @PathParam("errorType")
    final String errorType) throws WebApplicationException {
        getData(callType, errorType);
        return buildHttpResponseForCSVData();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax
     * .ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(@SuppressWarnings("unused")
    final MultivaluedMap<String, String> requestParameters) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the multiple ranking data.
     * 
     * @param requestParameters
     *          the request parameters
     * @return the multiple ranking data
     * @throws WebApplicationException
     *           the parse exception
     */
    public String getRequestData(final String requestId, final MultivaluedMap<String, String> requestParameters,
            final String callType, final String errorType) throws WebApplicationException {
        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        checkAndCreateINFOAuditLogEntryForURI(requestParameters);

        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(GRID_PARAM)) {
            return getGridResults(requestId, requestParameters, callType, errorType);
        }
        return getNoSuchDisplayErrorResponse(displayType);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.ericsson.eniq.events.server.resources.BaseResource#checkParameters(
     * javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected List<String> checkParameters(final MultivaluedMap<String, String> requestParameters) {
        final List<String> errors = new ArrayList<String>();

        if (!requestParameters.containsKey(TYPE_PARAM)) {
            errors.add(TYPE_PARAM);
        }
        if (!requestParameters.containsKey(DISPLAY_PARAM)) {
            errors.add(DISPLAY_PARAM);
        }
        final String type = requestParameters.getFirst(TYPE_PARAM);
        final String errorType = requestParameters.getFirst(ERROR_TYPE_PARAM);
        if (TOTAL_CALLS.equals(errorType) && !TYPE_INTERNAL_CAUSE_CODE.equals(type)) {
            errors.add(TYPE_PARAM);
        }
        return errors;
    }

    private TechPackTables getTechPackTablesOrViews(final FormattedDateTimeRange dateTimeRange, final String type,
            final String timerange, final String errorType) {
        if (shouldQueryUseAggregationView(type, timerange)) {
            return getAggregationTables(type, timerange, getTechPacksApplicableForType(), errorType);
        }
        return getRawTables(type, dateTimeRange, getTechPacksApplicableForType());
    }

    private TechPackTables getAggregationTables(final String type, final String timerange,
            final List<String> listOfTechPacks, final String errorType) {
        if (MssMultipleErrorType.isUnansweredCallsOrDurationCalls(errorType)) {
            final String time = ApplicationConstants.returnAggregateViewType(timerange);
            final AggregationTableInfo aggrTableInfo = aggregationViews.get(type);
            return getDurationAndUnansweredCallsAggTables(aggrTableInfo.getAggregationView(), time, type,
                    listOfTechPacks);
        }
        return getAggregationTables(type, timerange, getTechPacksApplicableForType());
    }

    private TechPackTables getDurationAndUnansweredCallsAggTables(final String aggregationViewForVoice,
            final String time, final String type, final List<String> listOfTechPacks) {
        final TechPackTables techPackTables = new TechPackTables(TableType.AGGREGATION);
        for (final String techPackName : listOfTechPacks) {
            if (isTechPackApplicableForType(type, techPackName)) {
                final TechPack techPack = new TechPack(techPackName, TableType.AGGREGATION,
                        getMatchingDIMTechpack(techPackName));
                final String sucVoiceTable = techPackName + UNDERSCORE + VOICE + UNDERSCORE + aggregationViewForVoice
                        + time;
                techPack.setSucAggregationView(sucVoiceTable);
                techPackTables.addTechPack(techPack);
            }
        }
        return techPackTables;
    }

    /**
     * If a particular type is restricted to only certain tech pack(s) (eg BSC is
     * only applicable for the EVENT_E_MSS tech pack) then this method returns
     * only that tech pack(s) Otherwise just returns complete list of tech packs
     * 
     * @return
     */
    private List<String> getTechPacksApplicableForType() {
        final List<String> listOfApplicableTechPacks = new ArrayList<String>();
        listOfApplicableTechPacks.add(EVENT_E_MSS_TPNAME);
        return listOfApplicableTechPacks;
    }

    /**
     * add various template parameters
     * 
     * @param requestParameters
     * @param templateParameters
     * @param timerange
     * @param type
     * @param techPackTables
     */
    // TODO - Consider Refactoring to baseResources with overloaded version
    // called
    // in each resource
    private void updateTemplateParameters(final MultivaluedMap<String, String> requestParameters,
            final Map<String, Object> templateParameters, final String timerange, final String type,
            final TechPackTables techPackTables, final String callType, final String errorType) {
        templateParameters.put(TYPE_PARAM, type);
        templateParameters.put(TIMERANGE_PARAM, timerange);
        templateParameters.put(COUNT_PARAM,
                getCountValue(requestParameters, MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE));
        templateParameters.put(TECH_PACK_TABLES, techPackTables);
        templateParameters.put(CALL_TYPE_PARAM, callType);
        templateParameters.put(ERROR_TYPE_PARAM, errorType);

        addColumnsForQueries(requestParameters.getFirst(TYPE_PARAM), templateParameters);
    }

    /**
     * Gets the grid results.
     * 
     * @param requestId
     *          corresponds to this request for cancelling later
     * @param requestParameters
     *          the request parameters
     * @param dateTimeRange
     *          the date time range
     * @return the grid results
     * @throws WebApplicationException
     *           the parse exception
     */
    private String getGridResults(final String requestId, final MultivaluedMap<String, String> requestParameters,
            final String callType, final String errorType) throws WebApplicationException {
        String type = requestParameters.getFirst(TYPE_PARAM);

        final FormattedDateTimeRange dateTimeRange = getDateTimeRange(requestParameters, errorType);
        final String timerange = getTimeRange(requestParameters, errorType);

        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);

        final Map<String, Object> templateParameters = new HashMap<String, Object>();
        final MultivaluedMap<String, String> tempParameters = new MultivaluedMapImpl();

        updateTempParametersWithRequestParameters(tempParameters, requestParameters, errorType);

        if (TYPE_IMSI.equals(type) || MssMultipleErrorType.isUnansweredCallsOrDurationCalls(errorType)) {
            type = tempParameters.getFirst(TYPE_PARAM);
        }

        final List<String> cxcLicensesForTechPack = techPackCXCMappingService.getTechPackCXCNumbers(EVENT_E_MSS_TPNAME);

        if (cxcLicensesForTechPack.isEmpty()) {
            return JSONUtils.createJSONErrorResult("MSS feature has not been installed.");
        }

        final TechPackTables techPackTables = getTechPackTablesOrViews(dateTimeRange, type, timerange, errorType);

        if (techPackTables.shouldReportErrorAboutEmptyRawTables()) {
            return JSONUtils.JSONEmptySuccessResult();
        }
        updateTemplateParameters(requestParameters, templateParameters, timerange, type, techPackTables, callType,
                errorType);

        final String template = getTemplate(RANKING_ANALYSIS, tempParameters, null, timerange, false);

        final String query = templateUtils.getQueryFromTemplate(template, templateParameters);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        final FormattedDateTimeRange newDateTimeRange = getNewDataTimeRange(type, dateTimeRange, timerange, errorType);

        // timeColumnIndexes is only used for remove ambiguous of streamDataAsCSV and getGridData overload
        final List<Integer> timeColumnIndexes = null;

        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, tzOffset, timeColumnIndexes, query, newDateTimeRange);
            return null;
        }

        Map<String, QueryParameter> queryParameters;
        queryParameters = this.queryUtils.mapRequestParametersForHashId(requestParameters, newDateTimeRange);

        return this.dataService.getGridData(requestId, query, queryParameters, timeColumnIndexes, tzOffset,
                getLoadBalancingPolicy(requestParameters));
    }

    /**
     * This method is used to get the newDataTimeRange, for IMSI, Duration Calls and Unanswered Calls Ranking, don't use offset.
     * @param type
     * @param dateTimeRange
     * @param timerange
     * @param errorType
     * @return
     */
    private final FormattedDateTimeRange getNewDataTimeRange(final String type,
            final FormattedDateTimeRange dateTimeRange, final String timerange, final String errorType) {
        if (MSS_IMSI_PARAM.equals(type) || MssMultipleErrorType.isUnansweredCallsOrDurationCalls(errorType)) {
            return dateTimeRange;
        }
        return getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange, getTechPacksApplicableForType());
    }

    /**
     * 
     * @param requestParameters
     * @param errorType
     * @return
     */
    private final FormattedDateTimeRange getDateTimeRange(final MultivaluedMap<String, String> requestParameters,
            final String errorType) {
        final String type = requestParameters.getFirst(TYPE_PARAM);
        if (TYPE_IMSI.equals(type) || MssMultipleErrorType.isUnansweredCallsOrDurationCalls(errorType)) {
            return getAndCheckFormattedDateTimeRange(requestParameters);
        }
        return getAndCheckFormattedDateTimeRangeForDailyAggregation(requestParameters, getTechPacksApplicableForType());
    }

    private final String getTimeRange(final MultivaluedMap<String, String> requestParameters, final String errorType) {
        final String type = requestParameters.getFirst(TYPE_PARAM);
        final FormattedDateTimeRange dateTimeRange = getDateTimeRange(requestParameters, errorType);
        final String timerange = getTimeRange(dateTimeRange);

        if (TYPE_IMSI.equals(type) || MssMultipleErrorType.isUnansweredCallsOrDurationCalls(errorType)) {
            if (EventDataSourceType.AGGREGATED_DAY.getValue().equals(timerange)) {
                return EventDataSourceType.AGGREGATED_15MIN.getValue();
            }
        }
        return timerange;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.MssBaseResource#getData(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Update the tempParameters to get the mapping template file name.
     * @param tempParameters
     * @param requestParameters
     */
    private void updateTempParametersWithRequestParameters(final MultivaluedMap<String, String> tempParameters,
            final MultivaluedMap<String, String> requestParameters, final String errorType) {
        // TODO Auto-generated method stub
        String type = requestParameters.getFirst(TYPE_PARAM);
        if (MssMultipleErrorType.isDurationCalls(errorType)) {
            type = type + UNDERSCORE + errorType;
            updateMssTypeParameter(type, requestParameters);
        }

        if (MssMultipleErrorType.isUnansweredCalls(errorType)) {
            type = errorType;
            updateMssTypeParameter(type, requestParameters);
        }

        updateMssTypeParameter(type, tempParameters);

        final String time = requestParameters.getFirst(TIME_QUERY_PARAM);
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);

        tempParameters.putSingle(TIME_QUERY_PARAM, time);
        tempParameters.putSingle(TZ_OFFSET, tzOffset);
    }

    private enum MssMultipleErrorType {
        DROPPED_CALLS_ET(DROPPED_CALLS), BLOCKED_CALLS_ET(BLOCKED_CALLS), TOTAL_CALLS_ET(TOTAL_CALLS), LONG_DURATION_CALLS_ET(
                LONG_DURATION_CALLS), SHORT_DURATION_CALLS_ET(SHORT_DURATION_CALLS), MS_TERMINATING_UNANSWERED_ET(
                MS_TERMINATING_UNANSWERED), MS_ORIGINATING_UNANSWERED_ET(MS_ORIGINATING_UNANSWERED);

        MssMultipleErrorType(final String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        protected static boolean checkValue(final String errorType) {
            for (final MssMultipleErrorType er : MssMultipleErrorType.values()) {
                if (er.value.equals(errorType)) {
                    return true;
                }
            }
            return false;
        }

        protected static boolean isDurationCalls(final String errorType) {
            if (SHORT_DURATION_CALLS_ET.value.equals(errorType) || LONG_DURATION_CALLS_ET.value.equals(errorType)) {
                return true;
            }
            return false;
        }

        protected static boolean isUnansweredCalls(final String errorType) {
            if (MS_TERMINATING_UNANSWERED_ET.value.equals(errorType)
                    || MS_ORIGINATING_UNANSWERED_ET.value.equals(errorType)) {
                return true;
            }
            return false;
        }

        protected static boolean isUnansweredCallsOrDurationCalls(final String errorType) {
            if (MssMultipleErrorType.isDurationCalls(errorType) || MssMultipleErrorType.isUnansweredCalls(errorType)) {
                return true;
            }
            return false;
        }

        protected final String value;
    }
}
