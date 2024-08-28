/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryConstants.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.common.tablesandviews.MssAggregationTableInfo;
import com.ericsson.eniq.events.server.common.tablesandviews.MssAggregationTableInfo.MSSAggregationType;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.queryconstructor.QueryConstructor;
import com.ericsson.eniq.events.server.utils.DateTimeRange;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * The Class MssEventVolumeResource.
 * This service returns event volume data for the UI widgets from Sybase IQ. 
 * The actual data comes from a normal JSON data source. 
 * 
 * @author ezhibhe
 * @since Jun 2011
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class MssEventVolumeResource extends MssBaseResource {

    @EJB
    protected QueryConstructor queryConstructor;

    private static final List<String> listOfTechPacks;

    private static final List<String> validTypes;

    private static final Map<MSSAggregationType, String> NO_TYPE_AGGR;

    private static final Map<MSSAggregationType, String> MSC_AGGR;

    private static final Map<MSSAggregationType, String> BSC_AGGR;

    private static final Map<MSSAggregationType, String> CELL_AGGR;

    private static final int INTERVAL_ONE_MINUTE = 1;

    private static final int INTERVAL_FIFTEEN_MINUTES = 15;

    private static final int INTERVAL_ONE_DAY_IN_MINUTES = 1440;

    static {
        listOfTechPacks = new ArrayList<String>();
        listOfTechPacks.add(EVENT_E_MSS);
        validTypes = Arrays.asList(TYPE_MSC, TYPE_BSC, TYPE_CELL);
        NO_TYPE_AGGR = new HashMap<MSSAggregationType, String>();
        MSC_AGGR = new HashMap<MSSAggregationType, String>();
        BSC_AGGR = new HashMap<MSSAggregationType, String>();
        CELL_AGGR = new HashMap<MSSAggregationType, String>();
        NO_TYPE_AGGR.put(MSSAggregationType.VOICE, EVNTSRC_EVENTID);
        NO_TYPE_AGGR.put(MSSAggregationType.LOC_SERVICE, "EVNTSRC");
        NO_TYPE_AGGR.put(MSSAggregationType.SMS, EVNTSRC_EVENTID);
        MSC_AGGR.put(MSSAggregationType.VOICE, EVNTSRC_EVENTID);
        MSC_AGGR.put(MSSAggregationType.LOC_SERVICE, "EVNTSRC");
        MSC_AGGR.put(MSSAggregationType.SMS, EVNTSRC_EVENTID);
        BSC_AGGR.put(MSSAggregationType.VOICE, VEND_HIER3_EVENTID);
        BSC_AGGR.put(MSSAggregationType.LOC_SERVICE, "VEND_HIER321");
        BSC_AGGR.put(MSSAggregationType.SMS, VEND_HIER3_EVENTID);
        CELL_AGGR.put(MSSAggregationType.VOICE, VEND_HIER321_EVENTID);
        CELL_AGGR.put(MSSAggregationType.LOC_SERVICE, "VEND_HIER321");
        CELL_AGGR.put(MSSAggregationType.SMS, VEND_HIER321_EVENTID);
    }

    public MssEventVolumeResource() {
        aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(NO_TYPE, new MssAggregationTableInfo(NO_TYPE_AGGR, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_MSC, new MssAggregationTableInfo(MSC_AGGR, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_BSC, new MssAggregationTableInfo(BSC_AGGR, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_CELL, new MssAggregationTableInfo(CELL_AGGR, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));

    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#getData(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }
        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(CHART_PARAM) || displayType.equals(GRID_PARAM)) {
            return getChartResults(requestId, requestParameters);
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

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        // it should contains node or groupname when it has type
        if (requestParameters.containsKey(TYPE_PARAM)) {
            if (!validTypes.contains(requestParameters.getFirst(TYPE_PARAM))) {
                return false;
            }
            if (!requestParameters.containsKey(NODE_PARAM) && !requestParameters.containsKey(GROUP_NAME_PARAM)) {
                return false;
            }
        }
        if (requestParameters.containsKey(NODE_PARAM) || requestParameters.containsKey(GROUP_NAME_PARAM)) {
            if (!queryUtils.checkValidValue(requestParameters)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Gets the chart results.
     *
     * @param requestId corresponds to this request for cancelling later
     * @param requestParameters - URL query parameters
     * @return JSON encoded results
     * @throws WebApplicationException the parse exception
     */
    private String getChartResults(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        if (!isValidValue(requestParameters)) {
            return JSONUtils.jsonErrorInputMsg();
        }

        final Map<String, Object> templateParameters = new HashMap<String, Object>();

        // timerange
        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(
                requestParameters, listOfTechPacks);
        final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();
        templateParameters.put(TIMERANGE_PARAM, timerange);

        // interval
        final int interval = getInterval(timerange);
        templateParameters.put(INTERVAL_PARAM, interval);

        // tz_offset, this values needn't put into tempalteParameters
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);

        // type If it is Network Event Volume, type is none and it uses NO_TYPE as its type
        String type = NO_TYPE;
        if (requestParameters.containsKey(TYPE_PARAM)) {
            type = requestParameters.getFirst(TYPE_PARAM);
        }
        templateParameters.put(TYPE_PARAM, type);
        // node It type is NO_TYPE, node is null
        templateParameters.put(NODE_PARAM, requestParameters.getFirst(NODE_PARAM));

        // group
        if (requestParameters.containsKey(GROUP_NAME_PARAM)) {
            templateParameters.put(GROUP_NAME_PARAM, requestParameters.get(GROUP_NAME_PARAM));
        }
        updateTemplateParametersWithGroupDefinition(templateParameters, requestParameters);

        // techpack & techpackRaw
        final TechPackTables techPackRawTables = getRawTables(type, dateTimeRange, listOfTechPacks);
        if (techPackRawTables.shouldReportErrorAboutEmptyRawTables()) {
            return JSONUtils.JSONEmptySuccessResult();
        }
        final TechPackTables techPackTables = getTechPackTablesOrViews(dateTimeRange, timerange, type, listOfTechPacks);
        if (techPackTables.shouldReportErrorAboutEmptyRawTables()) {
            return JSONUtils.JSONEmptySuccessResult();
        }

        templateParameters.put(TECH_PACK_TABLES, techPackTables);
        // raw tables are always used by Impact Subscribers
        templateParameters.put(TECH_PACK_RAW_TABLES, techPackRawTables);

        // starttime & endtime
        final FormattedDateTimeRange newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange,
                listOfTechPacks);
        templateParameters.put(START_TIME, String.format("'%s'", newDateTimeRange.getStartDateTime()));
        templateParameters.put(END_TIME, String.format("'%s'", newDateTimeRange.getEndDateTime()));

        final List<String> queries = queryConstructor.getMssEventVolumeQuery(templateParameters);

        if (isBlank(queries)) {
            return JSONUtils.JSONBuildFailureError();
        }

        checkAndCreateFineAuditLogEntryForQueries(requestParameters, queries, newDateTimeRange);

        final String[] eventVolumeDateTimeList = DateTimeRange.getSamplingTimeList(newDateTimeRange,
                DateTimeRange.getChartInterval(newDateTimeRange, timerange));

        if (requestParameters.containsKey(TYPE_PARAM)) {
            return getEventVolume(requestId, queries, requestParameters, newDateTimeRange, eventVolumeDateTimeList,
                    tzOffset, MSS_EVENT_VOLUME_X_AXIS_VALUE, MSS_EVENT_VOLUME_SECOND_Y_AXIS_VALUE,
                    MSS_EVENT_VOLUME_X_AXIS_VALUE);
        }
        return getEventVolume(requestId, queries, requestParameters, newDateTimeRange, eventVolumeDateTimeList,
                tzOffset, MSS_NETWORK_EVENT_VOLUME_X_AXIS_VALUE, null, MSS_NETWORK_EVENT_VOLUME_X_AXIS_VALUE);

    }

    /**
     * @param requestId corresponds to this request for cancelling later
     * @param query the sql query
     * @param requestParameters the UI request parameters
     * @param newDateTimeRange the formatted input time range
     * @param eventVolumeDateTimeList the event volume time list
     * @param tzOffset the time zone offset
     * @param The column index of time sample, which is used to compare with eventVolumeDateTimeList to get final result
     * @param The column index, which one should be removed when do maxY calculation
     * @param timeColumn the time column of event volume chart
     * 
     * @return event-volume data
     */
    private String getEventVolume(final String requestId, final List<String> queries,
            final MultivaluedMap<String, String> requestParameters, final FormattedDateTimeRange newDateTimeRange,
            final String[] eventVolumeDateTimeList, final String tzOffset, final String xAxis,
            final String secondYAxis, final String timeColumn) {
        return this.dataService.getSamplingChartDataWithSumCalculatorForInteger(requestId, queries,
                this.queryUtils.mapRequestParametersForHashId(requestParameters, newDateTimeRange),
                eventVolumeDateTimeList, xAxis, secondYAxis, timeColumn, tzOffset,
                getLoadBalancingPolicy(requestParameters));
    }

    /**
     * getInterval return interval according to timerange
     * @param timerange in minutes
     * @return
     */
    private int getInterval(final String timerange) {
        int rst = INTERVAL_ONE_MINUTE;
        if (FIFTEEN_MINUTES.equalsIgnoreCase(timerange)) {
            rst = INTERVAL_FIFTEEN_MINUTES;
        } else if (DAY.equalsIgnoreCase(timerange)) {
            rst = INTERVAL_ONE_DAY_IN_MINUTES;
        } else {
            rst = INTERVAL_ONE_MINUTE;
        }
        return rst;
    }

    private boolean isBlank(final List<String> queries) {
        boolean rst = false;
        if (queries.isEmpty()) {
            rst = true;
        } else {
            for (final String query : queries) {
                if (StringUtils.isBlank(query)) {
                    rst = true;
                    break;
                }
            }
        }
        return rst;
    }

    private void checkAndCreateFineAuditLogEntryForQueries(final MultivaluedMap<String, String> requestParameters,
            final List<String> queries, final FormattedDateTimeRange timerange) {
        for (final String query : queries) {
            checkAndCreateFineAuditLogEntryForQuery(requestParameters, query, timerange);
        }
    }

    /**
     * Test purpose
     * @param queryConstructor
     */
    public void setQueryConstructor(final QueryConstructor queryConstructor) {
        this.queryConstructor = queryConstructor;
    }
}