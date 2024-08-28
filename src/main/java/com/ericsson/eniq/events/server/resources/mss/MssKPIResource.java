/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;

import java.util.*;

import javax.ejb.*;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.common.TechPackData;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.kpi.MssKpiQueryFactoryHelper;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.utils.DateTimeRange;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * @since April 2011
 */
@Stateless
@LocalBean
public class MssKPIResource extends MssBaseResource {

    @EJB
    protected MssKpiQueryFactoryHelper mssQueryBuilder;

    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(NODE_PARAM) || requestParameters.containsKey(GROUP_NAME_PARAM)) {
            if (!queryUtils.checkValidValue(requestParameters)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters) throws WebApplicationException {
        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        checkAndCreateINFOAuditLogEntryForURI(requestParameters);

        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(CHART_PARAM) || displayType.equals(GRID_PARAM)) {
            return getChartResults(requestId, requestParameters);
        }
        return getNoSuchDisplayErrorResponse(displayType);
    }

    /**
     * Gets the JSON chart results for KPI. For Single KPI, service is sampling 30 results based on timerange and interval
     * 
     * @param requestId corresponds to this request for cancelling later
     * @param requestParameters - URL query parameters
     * @param dateTimeRange - formatted date time range
     * @return JSON encoded results
     * @throws WebApplicationException
     */
    private String getChartResults(final String requestId, final MultivaluedMap<String, String> requestParameters) throws WebApplicationException {
        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(requestParameters,
                TechPackData.completeDVTPTechPackList);
        final Map<String, Object> templateParameters = new HashMap<String, Object>();
        final String type = requestParameters.getFirst(TYPE_PARAM);
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
        templateParameters.put(TYPE_PARAM, type);
        final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();
        templateParameters.put(TIMERANGE_PARAM, timerange);
        if (requestParameters.containsKey(GROUP_NAME_PARAM)) {
            templateParameters.put(GROUP_NAME_KEY, requestParameters.get(GROUP_NAME_PARAM));
        }
        templateParameters.put(IS_EXCULDED_TAC_OR_TACGROUP, isExcludedTacOrGroup(requestParameters));
        updateTemplateParametersWithGroupDefinitionForHashId(templateParameters, requestParameters);

        if (!isValidValue(requestParameters)) {
            return JSONUtils.jsonErrorInputMsg();
        }

        final FormattedDateTimeRange newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange,
                TechPackData.completeDVTPTechPackList);
        updateTemplateParameterForTime(templateParameters, newDateTimeRange);

        final String[] kpiDateTimeList = DateTimeRange.getSamplingTimeList(newDateTimeRange,
                DateTimeRange.getChartInterval(newDateTimeRange, timerange));

        final TechPackTables techPackTables = getRawTables(type, newDateTimeRange);
        if (techPackTables.getRawTables().size() <= 0) {
            return JSONUtils.JSONEmptySuccessResult();
        }
        templateParameters.put(TECH_PACK_TABLES, techPackTables);

        if (requestParameters.get(GROUP_NAME_PARAM) != null) {
            templateParameters.put(GROUP_NAME_PARAM, requestParameters.get(GROUP_NAME_PARAM));
        }

        final List<String> queries = mssQueryBuilder.getMssKPIQuery(templateParameters);

        if (queries == null || queries.isEmpty()) {
            return JSONUtils.JSONBuildFailureError();
        }
        checkAndCreateFineAuditLogEntryForQuery(requestParameters, queries, newDateTimeRange);
        final Map<String, QueryParameter> querParams = this.queryUtils.mapRequestParametersForHashId(requestParameters, newDateTimeRange);
        return this.dataService.getSamplingChartData(requestId, queries, querParams, kpiDateTimeList, KPI_X_AXIS_VALUE, MSS_KPI_SECOND_Y_AXIS_VALUE,
                EVENT_TIME_COLUMN_INDEX, tzOffset, getLoadBalancingPolicy(requestParameters));
    }

    private void updateTemplateParameterForTime(final Map<String, Object> templateParameters, final FormattedDateTimeRange newDateTimeRange) {
        final StringBuffer startTime = new StringBuffer("'" + newDateTimeRange.getStartDateTime() + "'");
        templateParameters.put(START_TIME, startTime.toString());
        final StringBuffer endTime = new StringBuffer("'" + newDateTimeRange.getEndDateTime() + "'");
        templateParameters.put(END_TIME, endTime.toString());
    }

    @Override
    protected List<String> checkParameters(final MultivaluedMap<String, String> requestParameters) {
        final List<String> errors = new ArrayList<String>();

        if (!requestParameters.containsKey(TYPE_PARAM)) {
            errors.add(TYPE_PARAM);
        }
        if (!requestParameters.containsKey(DISPLAY_PARAM)) {
            errors.add(DISPLAY_PARAM);
        }
        return errors;
    }
}
