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
package com.ericsson.eniq.events.server.resources.mss.piechart;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.resources.mss.piechart.MSSCauseCodeAnalysisPieChartConstants.*;

import java.util.*;

import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.common.*;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.resources.mss.MssBaseResource;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * This is the main resource class for MSS cause code pie chart analysis which is used by the API class #MSSCauseCodeAnalysisPieChartAPI
 * 
 * @since 2011
 * 
 */
@Stateless
@LocalBean
public class MSSCauseCodeAnalysisPieChartResource extends MssBaseResource {

    private static final List<String> listOfTechPacks = new ArrayList<String>();

    private FormattedDateTimeRange dateTimeRange = null;

    private String timerange = null;

    private boolean useAggregations = false;

    static {
        listOfTechPacks.add(TechPackData.EVENT_E_MSS);
    }

    /**
     * Constructor : it initialises the aggregationViews with possible aggregations for different types
     */
    public MSSCauseCodeAnalysisPieChartResource() {
        aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_INTERNAL_CAUSE_CODE, new AggregationTableInfo(EVNTSRC_CC_FC, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_MSC, new AggregationTableInfo(EVNTSRC_CC_FC, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_BSC,
                new AggregationTableInfo(HIER3_CC_FC, EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
    }

    /**
     * Only public interface to the outside world. This method returns the cause code pie chart analysis data
     * 
     * @param path the URL path
     * 
     * @return the result of cause code pie chart analysis
     */
    public String getResults(final String path) {
        final MultivaluedMap<String, String> requestParameters = getDecodedQueryParameters();
        if (!isValidValue(requestParameters)) {
            return JSONUtils.jsonErrorInputMsg();
        }
        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        final String type = requestParameters.getFirst(TYPE_PARAM);
        return getPieResults(requestId, requestParameters, path, type);
    }

    /**
     * This is the main method that does the main work : the input (from URL) -> output (as JSON) This is a facade that uses other utility methods to
     * perform the end-to-end work
     * 
     * @param requestId the ID that comes with each request header with the URL call
     * @param requestParameters the parameters from the URL
     * @param path the relative URL
     * @param type the input from URL
     * 
     * @return the query result in JSON format
     * 
     * @throws WebApplicationException
     */
    private String getPieResults(final String requestId, final MultivaluedMap<String, String> requestParameters, final String path, final String type)
            throws WebApplicationException {
        final Map<String, Object> templateParameters = new HashMap<String, Object>();

        updateTemplateWithTimeRangeInfo(templateParameters, requestParameters);
        updateTemplateWithGroupOrColumnInfo(templateParameters, requestParameters, type);
        updateTemplateWithPathSpecificInfo(templateParameters, requestParameters, path);
        if (updateTemplateWithTechPackInfo(templateParameters, type)) {
            return JSONUtils.JSONEmptySuccessResult();
        }
        final String query = getQuery(requestParameters, templateParameters, path);

        return getData(requestParameters, requestId, query);
    }

    /**
     * This private utility method maps the input type parameter to the group type
     * 
     * @param type the input from URL
     * 
     * @return the mapped group type
     */
    private String getGroupType(final String type) {
        if (TYPE_BSC.equalsIgnoreCase(type)) {
            return GROUP_TYPE_HIER3;
        } else if (TYPE_CELL.equalsIgnoreCase(type)) {
            return GROUP_TYPE_HIER1;
        } else if (TYPE_MSC.equalsIgnoreCase(type)) {
            return GROUP_TYPE_EVENT_SRC_CS;
        }
        return null;
    }

    /**
     * This private utility method updates the template parameters with group or single node specific info
     * 
     * @param templateParameters the parameters used in template
     * @param requestParameters the parameters from the URL
     * @param type the input type parameter from URL
     */
    private void updateTemplateWithGroupOrColumnInfo(final Map<String, Object> templateParameters,
                                                     final MultivaluedMap<String, String> requestParameters, final String type) {
        if (requestParameters.containsKey(GROUP_NAME_PARAM)) {
            final Map<String, Group> groupDefs = dataService.getGroupsForTemplates();
            final Group groupDef = groupDefs.get(getGroupType(type));
            templateParameters.put(GROUP_TABLE_NAME, groupDef.getTableName());
            templateParameters.put(GROUP_COLUMN_NAME, groupDef.getGroupNameColumn());
            templateParameters.put(ID, mssGroupJoinKey.get(type));
            templateParameters.put(IS_GROUP, true);
        } else {
            templateParameters.put(COLUMNS, mssColumns.get(type));
            templateParameters.put(IS_GROUP, false);
        }
    }

    /**
     * This private utility method updates the template parameters with URL path specific info
     * 
     * @param templateParameters the parameters used in template
     * @param requestParameters the parameters from the URL
     * @param path the relative URL
     */
    private void updateTemplateWithPathSpecificInfo(final Map<String, Object> templateParameters,
                                                    final MultivaluedMap<String, String> requestParameters, final String path) {
        if (path.endsWith(CAUSE_CODE_ANALYSIS)) {
            templateParameters.put(CAUSE_CODE_IDS, requestParameters.getFirst(CAUSE_CODE_IDS));
        }
    }

    /**
     * This private utility method updates the template parameters with table info
     * 
     * @param templateParameters the parameters used in template
     * @param techPackTables the tables
     * @param key the template key
     */
    private void updateTemplateWithTableInfo(final Map<String, Object> templateParameters, final TechPackTables techPackTables, final String key) {
        final List<String> tables = new ArrayList<String>();
        tables.addAll(techPackTables.getErrTables());
        tables.addAll(techPackTables.getErrTables(KEY_TYPE_DROP_CALL));
        templateParameters.put(key, tables);
    }

    /**
     * This private utility method updates the template parameters with techpack specific info
     * 
     * @param templateParameters the parameters used in template
     * @param type the input type parameter from URL
     * 
     * @return false if list of tables is empty
     */
    private boolean updateTemplateWithTechPackInfo(final Map<String, Object> templateParameters, final String type) {
        final TechPackTables techPackTables = getTechPackTablesOrViews(dateTimeRange, timerange, type, listOfTechPacks);
        updateTemplateWithTableInfo(templateParameters, techPackTables, RAW_MSS_TABLES);

        useAggregations = shouldQueryUseAggregationView(type, timerange);
        templateParameters.put(USE_AGGREGATION_TABLES, useAggregations);
        if (useAggregations) {
            final TechPackTables techPackRawTables = getRawTables(type, dateTimeRange);
            updateTemplateWithTableInfo(templateParameters, techPackRawTables, RAW_ALL_ERR_TABLES);
            return techPackTables.shouldReportErrorAboutEmptyRawTables() || techPackRawTables.shouldReportErrorAboutEmptyRawTables();
        }

        return techPackTables.shouldReportErrorAboutEmptyRawTables();
    }

    /**
     * This private utility method updates the template parameters with time range specific info
     * 
     * @param templateParameters the parameters used in template
     * @param requestParameters the parameters from the URL
     */
    private void updateTemplateWithTimeRangeInfo(final Map<String, Object> templateParameters, final MultivaluedMap<String, String> requestParameters) {
        dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(requestParameters, TechPackData.completeDVTPTechPackList);
        timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();
        templateParameters.put(TIMERANGE_PARAM, timerange);
    }

    /**
     * This private utility method gets the actual query using the path and time range specific logic
     * 
     * @param templateParameters the parameters used in template
     * @param requestParameters the parameters from the URL
     * @param path the relative URL
     * 
     * @return the query that will be run in the DB
     */
    private String getQuery(final MultivaluedMap<String, String> requestParameters, final Map<String, Object> templateParameters, final String path) {
        if (useAggregations && (path.endsWith(CAUSE_CODE_ANALYSIS) || path.endsWith(SUB_CAUSE_CODE_ANALYSIS))) {

            final String queryEvents = templateUtils.getQueryFromTemplate(
                    getTemplate(path + PATH_SEPARATOR + EVENT_ANALYSIS, requestParameters, null), templateParameters);

            final String querySubscribers = templateUtils.getQueryFromTemplate(
                    getTemplate(path + PATH_SEPARATOR + IMPACTED_SUBSCRIBERS.toUpperCase(), requestParameters, null), templateParameters);

            templateParameters.put(templateParameterEventsQuery, queryEvents);
            templateParameters.put(templateParameterSubscribersQuery, querySubscribers);

            return templateUtils.getQueryFromTemplate(getTemplate(path + PATH_SEPARATOR + AGGREGATION_TABLES, requestParameters, null),
                    templateParameters);
        }
        return templateUtils.getQueryFromTemplate(getTemplate(path, requestParameters, null), templateParameters);
    }

    /**
     * This private utility method gets the result from the database and returns it in JSON format
     * 
     * @param requestParameters the parameters from the URL
     * @param requestId the ID that comes with each request header with the URL call
     * @param query the SQL to be run to get the result
     * 
     * @return the result in JSON format
     */
    private String getData(final MultivaluedMap<String, String> requestParameters, final String requestId, final String query) {
        final FormattedDateTimeRange newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange,
                TechPackData.completeDVTPTechPackList);
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
        final String timeColumn = null;
        return this.dataService.getGridData(requestId, query, this.queryUtils.mapRequestParametersForHashId(requestParameters, newDateTimeRange),
                timeColumn, tzOffset, getLoadBalancingPolicy(requestParameters));
    }

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
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<String> checkParameters(final MultivaluedMap<String, String> requestParameters) {
        final List<String> errors = new ArrayList<String>();

        if (getGroupType(requestParameters.getFirst(TYPE_PARAM)) == null) {
            errors.add(TYPE_PARAM);
        }
        return errors;
    }
}