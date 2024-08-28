/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.*;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.utils.SubsessionBIUtils.*;

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

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.common.MediaTypeConstants;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.json.JSONException;
import com.ericsson.eniq.events.server.queryconstructor.QueryConstructor;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class MssSubsessionBIResource extends MssBaseResource {

    @EJB
    protected QueryConstructor queryConstructor;

    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(GROUP_NAME_PARAM) || isImsiOrImsiGroupQuery(requestParameters)
                || requestParameters.containsKey(NODE_PARAM) || (requestParameters.containsKey(MSISDN_PARAM))) {

            if (!queryUtils.checkValidValue(requestParameters)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<String> checkParameters(final MultivaluedMap<String, String> requestParameters) {
        final List<String> errors = new ArrayList<String>();
        if (!requestParameters.containsKey(DISPLAY_PARAM)) {
            errors.add(DISPLAY_PARAM);
        }
        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType == null || !(displayType.equals(GRID_PARAM) || displayType.equals(CHART_PARAM))) {
            errors.add(DISPLAY_PARAM);
        }
        return errors;
    }

    /**
     * Gets the Busy Day data for SUB BI.
     *
     * @return the Busy Day data for SUB BI
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path(SUBBI_BUSY_DAY)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getSubBIBusyDayData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        final MultivaluedMap<String, String> requestParameters = getDecodedQueryParameters();
        final String errResponse = getErrorResponse(requestParameters);
        if (errResponse != null) {
            return errResponse;
        }
        checkAndCreateINFOAuditLogEntryForURI(requestParameters);
        final Map<String, Object> templateParameters = updateTemplateParams(requestParameters);
        final String emptyRawTableResponse = getEmptyRawTableResponse(templateParameters);
        if (emptyRawTableResponse != null) {
            return emptyRawTableResponse;
        }
        final List<String> allqueries = queryConstructor.getMssSubscriberBIBusyDayQuery(templateParameters);
        if (allqueries.isEmpty()) {
            return JSONUtils.JSONBuildFailureError();
        }
        final FormattedDateTimeRange dateTimeRange = (FormattedDateTimeRange) templateParameters
                .get(FORMATTED_DATE_TIME_RANGE);
        checkAndCreateFineAuditLogEntryForQuery(requestParameters, allqueries, dateTimeRange);
        final List<Integer> timeColumnIndexes = getTimeColumnIndexes(requestParameters, templateParameters);
        return getSubBusyDayHourAndCellDataResults(requestId, requestParameters, allqueries, dateTimeRange,
                timeColumnIndexes, SUBBI_BUSY_DAY);
    }

    /**
     * Gets the Busy Hour data for SUB BI.
     *
     * @return the Busy Hour data for SUB BI
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path(SUBBI_BUSY_HOUR)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getSubBIBusyHourData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        final MultivaluedMap<String, String> requestParameters = getDecodedQueryParameters();
        final String errResponse = getErrorResponse(requestParameters);
        if (errResponse != null) {
            return errResponse;
        }
        checkAndCreateINFOAuditLogEntryForURI(requestParameters);
        final Map<String, Object> templateParameters = updateTemplateParams(requestParameters);
        final String emptyRawTableResponse = getEmptyRawTableResponse(templateParameters);
        if (emptyRawTableResponse != null) {
            return emptyRawTableResponse;
        }
        final List<String> allqueries = queryConstructor.getMssSubscriberBIBusyHourQuery(templateParameters);
        if (allqueries.isEmpty()) {
            return JSONUtils.JSONBuildFailureError();
        }
        final FormattedDateTimeRange dateTimeRange = (FormattedDateTimeRange) templateParameters
                .get(FORMATTED_DATE_TIME_RANGE);
        final List<Integer> timeColumnIndexes = getTimeColumnIndexes(requestParameters, templateParameters);
        checkAndCreateFineAuditLogEntryForQuery(requestParameters, allqueries, dateTimeRange);
        return getSubBusyDayHourAndCellDataResults(requestId, requestParameters, allqueries, dateTimeRange,
                timeColumnIndexes, SUBBI_BUSY_HOUR);
    }

    /**
     * Gets the Failure data for SUB BI.
     *
     * @return the Failure data for SUB BI
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path(SUBBI_FAILURE)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getSubBIFailureData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        final MultivaluedMap<String, String> requestParameters = getDecodedQueryParameters();
        final String errResponse = getErrorResponse(requestParameters);
        if (errResponse != null) {
            return errResponse;
        }
        checkAndCreateINFOAuditLogEntryForURI(requestParameters);
        final Map<String, Object> templateParameters = updateTemplateParams(requestParameters);
        final String emptyRawTableResponse = getEmptyRawTableResponse(templateParameters);
        if (emptyRawTableResponse != null) {
            return emptyRawTableResponse;
        }
        final String evntId = (String) templateParameters.get(EVENT_ID_PARAM);
        List<Integer> timeColumnIndexes = null;
        if (evntId != null) {
            timeColumnIndexes = getEventTypeSpecificTimeColumns(evntId.trim());
        }
        final List<String> allqueries = queryConstructor.getMssSubscriberBIFailuresQuery(templateParameters);
        if (allqueries.isEmpty()) {
            return JSONUtils.JSONBuildFailureError();
        }
        final FormattedDateTimeRange dateTimeRange = (FormattedDateTimeRange) templateParameters
                .get(FORMATTED_DATE_TIME_RANGE);
        checkAndCreateFineAuditLogEntryForQuery(requestParameters, allqueries, dateTimeRange);
        return getChartDataResults(requestId, requestParameters, allqueries, dateTimeRange, timeColumnIndexes);
    }

    /**
     * Gets the Failure data for SUB BI.
     *
     * @return the Failure data for SUB BI
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path(SUBBI_CELL)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getSubBICellAnalysisData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        final MultivaluedMap<String, String> requestParameters = getDecodedQueryParameters();
        final String errResponse = getErrorResponse(requestParameters);
        if (errResponse != null) {
            return errResponse;
        }
        checkAndCreateINFOAuditLogEntryForURI(requestParameters);
        final Map<String, Object> templateParameters = updateTemplateParams(requestParameters);
        final String emptyRawTableResponse = getEmptyRawTableResponse(templateParameters);
        if (emptyRawTableResponse != null) {
            return emptyRawTableResponse;
        }
        final List<String> allqueries = queryConstructor.getMssSubscriberBICellAnalysisQuery(templateParameters);
        if (allqueries.isEmpty()) {
            return JSONUtils.JSONBuildFailureError();
        }
        final FormattedDateTimeRange dateTimeRange = (FormattedDateTimeRange) templateParameters
                .get(FORMATTED_DATE_TIME_RANGE);
        final List<Integer> timeColumnIndexes = getTimeColumnIndexes(requestParameters, templateParameters);
        checkAndCreateFineAuditLogEntryForQuery(requestParameters, allqueries, dateTimeRange);
        return getSubBusyDayHourAndCellDataResults(requestId, requestParameters, allqueries, dateTimeRange,
                timeColumnIndexes, SUBBI_CELL);
    }

    /**
     * Gets the Failure data for SUB BI.
     *
     * @return the Failure data for SUB BI
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path(SUBBI_TERMINAL)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getSubBITerminalAnalysisData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        final MultivaluedMap<String, String> requestParameters = getDecodedQueryParameters();
        final String errResponse = getErrorResponse(requestParameters);
        if (errResponse != null) {
            return errResponse;
        }
        checkAndCreateINFOAuditLogEntryForURI(requestParameters);
        final Map<String, Object> templateParameters = updateTemplateParams(requestParameters);
        final String emptyRawTableResponse = getEmptyRawTableResponse(templateParameters);
        if (emptyRawTableResponse != null) {
            return emptyRawTableResponse;
        }
        final List<String> allqueries = queryConstructor.getMssSubscriberBITerminalAnalysisQuery(templateParameters);
        if (allqueries.isEmpty()) {
            return JSONUtils.JSONBuildFailureError();
        }
        final FormattedDateTimeRange dateTimeRange = (FormattedDateTimeRange) templateParameters
                .get(FORMATTED_DATE_TIME_RANGE);
        final List<Integer> timeColumnIndexes = getTimeColumnIndexes(requestParameters, templateParameters);
        checkAndCreateFineAuditLogEntryForQuery(requestParameters, allqueries, dateTimeRange);
        return getGridDataResults(requestId, requestParameters, allqueries, dateTimeRange, timeColumnIndexes);
    }

    /**
     * Gets the Failure data for SUB BI.
     *
     * @return the Failure data for SUB BI
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path(SUBBI_SUBSCRIBER_DETAILS)
    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    public String getSubBISubscriberDetailsData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        final MultivaluedMap<String, String> requestParameters = getDecodedQueryParameters();
        final String errResponse = getErrorResponse(requestParameters);
        if (errResponse != null) {
            return errResponse;
        }
        checkAndCreateINFOAuditLogEntryForURI(requestParameters);
        final Map<String, Object> templateParameters = updateTemplateParams(requestParameters);
        final String emptyRawTableResponse = getEmptyRawTableResponse(templateParameters);
        if (emptyRawTableResponse != null) {
            return emptyRawTableResponse;
        }
        final List<Integer> timeColumnIndexes = new ArrayList<Integer>();
        timeColumnIndexes.add(8);
        timeColumnIndexes.add(9);
        final List<String> allqueries = queryConstructor.getMssSubscriberDetailsQuery(templateParameters);
        if (allqueries.isEmpty()) {
            return JSONUtils.JSONBuildFailureError();
        }
        final FormattedDateTimeRange dateTimeRange = (FormattedDateTimeRange) templateParameters
                .get(FORMATTED_DATE_TIME_RANGE);
        checkAndCreateFineAuditLogEntryForQuery(requestParameters, allqueries, dateTimeRange);
        return getDrillDownGridData(requestId, requestParameters, allqueries, dateTimeRange, timeColumnIndexes);
    }

    /**
     * This method list the time column indexes in event analysis report for each event type
     * @param requestParameters url parameters
     * @param templateParameters map of parameters required to generate query
     * @return List of time column indexes if KEY is ERR else null
     */
    private List<Integer> getTimeColumnIndexes(final MultivaluedMap<String, String> requestParameters,
            final Map<String, Object> templateParameters) {
        final String key = requestParameters.getFirst(KEY_PARAM);
        List<Integer> timeColumnIndexes = null;
        if (key != null && KEY_TYPE_ERR.equals(key)) {
            final String evntId = (String) templateParameters.get(EVENT_ID_PARAM);
            String timeColToFetch = "ALL";
            if (evntId != null) {
                timeColToFetch = evntId.trim();
            }
            timeColumnIndexes = getEventTypeSpecificTimeColumns(timeColToFetch);
        }
        return timeColumnIndexes;
    }

    /**
     * This method is used to validate the parameters and return appropriate error message
     * to the UI
     * @param requestParameters url parameters
     * @return null if required parameters are present else error message
     */
    private String getErrorResponse(final MultivaluedMap<String, String> requestParameters) {
        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }
        if (!isValidValue(requestParameters)) {
            return JSONUtils.jsonErrorInputMsg();
        }
        return null;
    }

    /**
     * This method is used to check if any raw tables exists
     * @param requestParameters url parameters
     * @return null if raw tables are present else empty response
     */
    private String getEmptyRawTableResponse(final Map<String, Object> templateParameters) {
        final TechPackTables techPackTables = (TechPackTables) templateParameters.get(TECH_PACK_TABLES);
        if (techPackTables == null || techPackTables.getRawTables().size() <= 0) {
            return JSONUtils.JSONEmptySuccessResult();
        }
        return null;
    }

    /**
     * This method will update the template parameters map with all the data needed to
     * generate different queries
     * @param requestParameters url parameters
     * @return templateParameters map of parameters required to generate query
     */
    private final Map<String, Object> updateTemplateParams(final MultivaluedMap<String, String> requestParameters) {
        final Map<String, Object> templateParameters = new HashMap<String, Object>();
        //Since the type is always IMSI or MSISDN, we need to exclude tac always
        templateParameters.put(IS_EXCULDED_TAC_OR_TACGROUP, false);
        templateParameters.put(COUNT_PARAM, getCountValue(requestParameters, MAXIMUM_POSSIBLE_GRID_DATA_ROW_COUNTS));
        //Only raw tables are used for subscriber intelligence. Hence no offsets are required
        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRange(requestParameters);
        templateParameters.put(FORMATTED_DATE_TIME_RANGE, dateTimeRange);
        updateTemplateParametersWithGroupDefinitionForHashId(templateParameters, requestParameters);
        final String type = requestParameters.getFirst(TYPE_PARAM);
        templateParameters.put(TYPE_PARAM, type);
        final String key = requestParameters.getFirst(KEY_PARAM);
        templateParameters.put(KEY_PARAM, key);
        final String eventId = requestParameters.getFirst(EVENT_ID_PARAM);
        templateParameters.put(EVENT_ID_PARAM, eventId);
        final TechPackTables techPackTables = getRawTables(type, dateTimeRange);
        templateParameters.put(TECH_PACK_TABLES, techPackTables);
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
        templateParameters.put(TZ_OFFSET, tzOffset);
        if (requestParameters.containsKey(GROUP_NAME_PARAM)) {
            templateParameters.put(GROUP_NAME_KEY, requestParameters.get(GROUP_NAME_PARAM));
        }
        updateEventNameParam(requestParameters, templateParameters);
        updateHourParam(requestParameters, templateParameters);
        updateDayParam(requestParameters, templateParameters);
        updateCellParam(requestParameters, templateParameters);
        updateTerminalAnalysisParam(requestParameters, templateParameters);
        return templateParameters;
    }

    /**
     * This method update the templateParameters with eventName specific data if present
     * in URL
     * @param requestParameters url parameters
     * @param templateParameters templateParameters with eventName specific data if present
     */
    private void updateEventNameParam(final MultivaluedMap<String, String> requestParameters,
            final Map<String, Object> templateParameters) {
        final String eventName = requestParameters.getFirst(EVENT_NAME_PARAM);
        if (StringUtils.isNotBlank(eventName)) {
            templateParameters.put(EVENT_NAME_PARAM, eventName);
            final String[] value = eventName.split(DELIMITER);
            templateParameters.put(EVENT_ID_PARAM, value[1]);
            templateParameters.put(KEY_PARAM, KEY_TYPE_ERR);
        }
    }

    /**
     * This method update the templateParameters with HOUR specific data if present
     * in URL
     * @param requestParameters url parameters
     * @param templateParameters templateParameters with HOUR specific data if present
     */
    private void updateHourParam(final MultivaluedMap<String, String> requestParameters,
            final Map<String, Object> templateParameters) {
        final String hourParam = requestParameters.getFirst(HOUR_PARAM);
        if (StringUtils.isNotBlank(hourParam)) {
            templateParameters.put(HOUR_PARAM, hourParam);
            final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
            final String hourWithTz = getHourParameterWithTZOffset(hourParam, tzOffset);
            requestParameters.putSingle(HOUR_PARAM, hourWithTz);
            updateKeyType(requestParameters, templateParameters);
        }
    }

    private void updateKeyType(final MultivaluedMap<String, String> requestParameters,
            final Map<String, Object> templateParameters) {
        final String evntId = requestParameters.getFirst(EVENT_ID_PARAM);
        if (evntId == null) {
            templateParameters.put(KEY_PARAM, KEY_TYPE_SUM);
            requestParameters.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        } else {
            templateParameters.put(KEY_PARAM, KEY_TYPE_ERR);
            requestParameters.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        }
    }

    /**
     * This method update the templateParameters with DAY specific data if present
     * in URL
     * @param requestParameters url parameters
     * @param templateParameters templateParameters with DAY specific data if present
     */
    private void updateDayParam(final MultivaluedMap<String, String> requestParameters,
            final Map<String, Object> templateParameters) {
        final String dayParam = requestParameters.getFirst(DAY_PARAM);
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
        templateParameters.put(TZ_OFFSET, getTZOffsetMinutes(tzOffset));
        if (StringUtils.isNotBlank(dayParam)) {
            templateParameters.put(DAY_PARAM, dayParam);
            updateKeyType(requestParameters, templateParameters);
        }
    }

    /**
     * This method update the templateParameters with cell specific data if present
     * in URL
     * @param requestParameters url parameters
     * @param templateParameters templateParameters with cell specific data if present
     */
    private void updateCellParam(final MultivaluedMap<String, String> requestParameters,
            final Map<String, Object> templateParameters) {
        final String nodeParam = requestParameters.getFirst(NODE_PARAM);
        final String hier321Id = requestParameters.getFirst(CELL_SQL_ID);
        if (StringUtils.isNotBlank(nodeParam)) {
            final long cellSqlId = queryUtils.createHashIdForCell(nodeParam);
            templateParameters.put(SUBBI_CELL_SQL_ID, cellSqlId);
            requestParameters.putSingle(CELL_SQL_ID, Long.toString(cellSqlId));
            final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
            templateParameters.put(TZ_OFFSET, getTZOffsetMinutes(tzOffset));
            updateKeyType(requestParameters, templateParameters);
        } else if (StringUtils.isNotBlank(hier321Id)) {
            templateParameters.put(SUBBI_CELL_SQL_ID, hier321Id);
            updateKeyType(requestParameters, templateParameters);
        }
    }

    /**
     * This method update the templateParameters with terminal specific data if present
     * in URL
     * @param requestParameters url parameters
     * @param templateParameters templateParameters with terminal specific data if present
     */
    private void updateTerminalAnalysisParam(final MultivaluedMap<String, String> requestParameters,
            final Map<String, Object> templateParameters) {
        final String tacParam = requestParameters.getFirst(TAC_PARAM);
        if (StringUtils.isNotBlank(tacParam)) {
            templateParameters.put(IS_SUBBI_TERMINAL_ANALYSIS, true);
            templateParameters.put(TAC_PARAM, tacParam);
            final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
            templateParameters.put(TZ_OFFSET, getTZOffsetMinutes(tzOffset));
            templateParameters.put(KEY_PARAM, KEY_TYPE_ERR);
            requestParameters.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        }
    }

    /**
     * This method is used by busy day and busy hour request to generate the chart or grid data
     * @param requestId unique id of the URL request
     * @param requestParameters url parameters
     * @param query query to execute
     * @param dateTimeRange time range data
     * @param timeColumnIndexes list of column to convert to local time
     * @param busyKey to identify busy hour or busy day request
     * @return json result for the URL request
     */
    private String getSubBusyDayHourAndCellDataResults(final String requestId,
            final MultivaluedMap<String, String> requestParameters, final List<String> queries,
            final FormattedDateTimeRange dateTimeRange, final List<Integer> timeColumnIndexes, final String busyKey) {
        if (isDrillType(requestParameters)) {
            return getDrillDownGridData(requestId, requestParameters, queries, dateTimeRange, timeColumnIndexes);
        }
        return this.dataService.getSubBIBusyDataWithAppendedRows(requestId, queries,
                this.queryUtils.mapRequestParametersForHashId(requestParameters, dateTimeRange),
                getLoadBalancingPolicy(requestParameters), busyKey, requestParameters.getFirst(TZ_OFFSET));
    }

    /**
     * This method is used to generate the chart or grid data
     * @param requestId unique id of the URL request
     * @param requestParameters url parameters
     * @param query query to execute
     * @param dateTimeRange time range data
     * @param timeColumnIndexes list of column to convert to local time
     * @param busyKey to identify busy hour or busy day request
     * @return json result for the URL request
     */
    private String getChartDataResults(final String requestId, final MultivaluedMap<String, String> requestParameters,
            final List<String> queries, final FormattedDateTimeRange dateTimeRange,
            final List<Integer> timeColumnIndexes) {
        if (isDrillType(requestParameters)) {
            return getDrillDownGridData(requestId, requestParameters, queries, dateTimeRange, timeColumnIndexes);
        }
        return this.dataService.getChartDataWithAppendedRows(requestId, queries,
                this.queryUtils.mapRequestParametersForHashId(requestParameters, dateTimeRange), SUBBI_X_AXIS_VALUE,
                null, null, requestParameters.getFirst(TZ_OFFSET), getLoadBalancingPolicy(requestParameters));
    }

    /**
     * This method is used to generate the chart or grid data
     * @param requestId unique id of the URL request
     * @param requestParameters url parameters
     * @param query query to execute
     * @param dateTimeRange time range data
     * @param timeColumnIndexes list of column to convert to local time
     * @param busyKey to identify busy hour or busy day request
     * @return json result for the URL request
     */
    private String getGridDataResults(final String requestId, final MultivaluedMap<String, String> requestParameters,
            final List<String> queries, final FormattedDateTimeRange dateTimeRange,
            final List<Integer> timeColumnIndexes) {
        if (isDrillType(requestParameters)) {
            return getDrillDownGridData(requestId, requestParameters, queries, dateTimeRange, timeColumnIndexes);
        }
        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, requestParameters.getFirst(TZ_OFFSET), timeColumnIndexes, queries,
                    dateTimeRange);
            return null;
        }
        return this.dataService.getGridDataWithAppendedRows(requestId, queries,
                this.queryUtils.mapRequestParametersForHashId(requestParameters, dateTimeRange), timeColumnIndexes,
                requestParameters.getFirst(TZ_OFFSET), getLoadBalancingPolicy(requestParameters));
    }

    private String getDrillDownGridData(final String requestId, final MultivaluedMap<String, String> requestParameters,
            final List<String> queries, final FormattedDateTimeRange dateTimeRange,
            final List<Integer> timeColumnIndexes) {
        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, requestParameters.getFirst(TZ_OFFSET), timeColumnIndexes, queries,
                    dateTimeRange);
            return null;
        }
        return this.dataService.getGridDataWithAppendedRows(requestId, queries,
                this.queryUtils.mapRequestParametersForHashId(requestParameters, dateTimeRange), timeColumnIndexes,
                requestParameters.getFirst(TZ_OFFSET), getLoadBalancingPolicy(requestParameters));
    }

    /**
     * This method is used to identify whether the request is a drill down from the parent window
     * @param requestParameters url parameters
     * @return true if HOUR, DAY or eventName param is present
     */
    private boolean isDrillType(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(HOUR_PARAM) || requestParameters.containsKey(DAY_PARAM)
                || requestParameters.containsKey(EVENT_NAME_PARAM) || requestParameters.containsKey(NODE_PARAM)
                || requestParameters.containsKey(KEY_PARAM)) {
            return true;
        }
        return false;
    }
}
