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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.common.tablesandviews.MssAggregationTableInfo;
import com.ericsson.eniq.events.server.common.tablesandviews.MssAggregationTableInfo.MSSAggregationType;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author echimma
 * @since 2011
 * 
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class MssKPIRatioResource extends MssBaseResource {
    private static final Map<MSSAggregationType, String> MSC_AGGR;

    private static final Map<MSSAggregationType, String> BSC_AGGR;

    static {
        MSC_AGGR = new HashMap<MSSAggregationType, String>();
        MSC_AGGR.put(MSSAggregationType.VOICE, EVENTID_EVNTSRC_HIER3);
        MSC_AGGR.put(MSSAggregationType.SMS, EVENTID_EVNTSRC_HIER3);
        MSC_AGGR.put(MSSAggregationType.LOC_SERVICE, EVENTID_EVNTSRC_HIER3);

        BSC_AGGR = new HashMap<MSSAggregationType, String>();
        BSC_AGGR.put(MSSAggregationType.VOICE, VEND_HIER321_EVENTID);
        BSC_AGGR.put(MSSAggregationType.SMS, VEND_HIER321_EVENTID);
        BSC_AGGR.put(MSSAggregationType.LOC_SERVICE, VEND_HIER321);
    }

    public MssKPIRatioResource() {
        aggregationViews = new HashMap<String, AggregationTableInfo>();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax
     * .ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        // TODO Auto-generated method stub
        if (!requestParameters.containsKey(TYPE_PARAM)) {
            return false;
        }
        if (!requestParameters.containsKey(DISPLAY_PARAM)) {
            return false;
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.ericsson.eniq.events.server.resources.BaseResource#getData(java.lang
     * .String, javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }
        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(GRID_PARAM)) {
            return getGridResults(requestId, requestParameters);
        }
        return getNoSuchDisplayErrorResponse(displayType);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.ericsson.eniq.events.server.resources.BaseResource#checkParameters
     * (javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected List<String> checkParameters(final MultivaluedMap<String, String> requestParameters) {
        final List<String> errors = new ArrayList<String>();
        if (!isValidValue(requestParameters)) {
            if (!requestParameters.containsKey(TYPE_PARAM)) {
                errors.add(TYPE_PARAM);
            }
            if (!requestParameters.containsKey(DISPLAY_PARAM)) {
                errors.add(DISPLAY_PARAM);
            }
        }
        return errors;
    }

    /**
     * getGridResults().
     * @param requestId
     * @param requestParameters
     * @return
     * @throws WebApplicationException
     */
    private String getGridResults(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {

        final Map<String, Object> templateParameters = new HashMap<String, Object>();

        List<Integer> timeColumnIndexes = null;

        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(
                requestParameters, getTechPacksApplicableForType());
        FormattedDateTimeRange newDateTimeRange = dateTimeRange;
        final String timerange = getTimeRange(dateTimeRange);

        final String type = requestParameters.getFirst(TYPE_PARAM);
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
        final String drilltype = queryUtils.getDrillTypeForMss(requestParameters);

        final MultivaluedMap<String, String> tempParameters = new MultivaluedMapImpl();

        String templateName = null;

        updateTempParametersWithRequestParameters(tempParameters, requestParameters);

        updateTemplateParemetersWithRequestParameters(templateParameters, requestParameters);

        String templateMappingDrillType = drilltype;

        if (!drilltype.equals(EVENTS_DRILL_TYPE_PARAM)) {
            templateParameters.put(DRILLTYPE_PARAM, drilltype);
            newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange,
                    getTechPacksApplicableForType());
            templateParameters.put(TIMERANGE_PARAM, timerange);

            final TechPackTables techPackTables = getTechPackTablesOrViews(dateTimeRange, timerange, type, drilltype);
            if (techPackTables.shouldReportErrorAboutEmptyRawTables()) {
                return JSONUtils.JSONEmptySuccessResult();
            }
            templateParameters.put(TECH_PACK_TABLES, techPackTables);
            templateName = getTemplate(KPI_RATIO, tempParameters, templateMappingDrillType, timerange, false);
        } else {
            templateMappingDrillType = drilltype;

            templateParameters.put(COUNT_PARAM,
                    getCountValue(requestParameters, MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE));
            final TechPackTables techPackTables = getRawTables(type, dateTimeRange, getTechPacksApplicableForType());
            if (techPackTables.shouldReportErrorAboutEmptyRawTables()) {
                return JSONUtils.JSONEmptySuccessResult();
            }
            templateParameters.put(TECH_PACK_TABLES, techPackTables);
            final String eventId = requestParameters.getFirst(EVENT_ID_PARAM);
            timeColumnIndexes = getEventTypeSpecificTimeColumns(eventId);

            //No aggregation table for Event Analysis here.
            templateName = getTemplate(KPI_RATIO, tempParameters, templateMappingDrillType);
        }

        final String query = templateUtils.getQueryFromTemplate(templateName, templateParameters);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, tzOffset, timeColumnIndexes, query, newDateTimeRange);
            return null;
        }

        final Map<String, QueryParameter> querParams = this.queryUtils.mapRequestParametersForHashId(requestParameters,
                newDateTimeRange);

        return this.dataService.getGridData(requestId, query, querParams, timeColumnIndexes, tzOffset,
                getLoadBalancingPolicy(requestParameters));
    }

    /**
     * Update the tempParameters to get the mapping template file name.
     * @param tempParameters
     * @param requestParameters
     */
    private void updateTempParametersWithRequestParameters(final MultivaluedMap<String, String> tempParameters,
            final MultivaluedMap<String, String> requestParameters) {
        // TODO Auto-generated method stub
        final String type = requestParameters.getFirst(TYPE_PARAM);
        updateMssTypeParameter(type, tempParameters);
        final String node = requestParameters.getFirst(NODE_PARAM);
        final String eventID = requestParameters.getFirst(EVENT_ID_PARAM);
        final String time = requestParameters.getFirst(TIME_QUERY_PARAM);
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);

        tempParameters.putSingle(NODE_PARAM, node);
        tempParameters.putSingle(EVENT_ID_PARAM, eventID);
        tempParameters.putSingle(TIME_QUERY_PARAM, time);
        tempParameters.putSingle(TZ_OFFSET, tzOffset);
    }

    /**
     * Update Template Paremeters With Request Parameters.
     * @param templateParameters
     * @param requestParameters
     */
    private void updateTemplateParemetersWithRequestParameters(final Map<String, Object> templateParameters,
            final MultivaluedMap<String, String> requestParameters) {
        final String type = requestParameters.getFirst(TYPE_PARAM);
        final String drilltype = queryUtils.getDrillType(requestParameters);
        final int count = getCountValue(requestParameters, MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE);
        templateParameters.put(COUNT_PARAM, count);
        updateTemplateParametersWithGroupDefinition(templateParameters, requestParameters);
        templateParameters.put(TYPE_PARAM, type);
        templateParameters.put(DRILLTYPE_PARAM, drilltype);
        templateParameters.put(EVENT_ID_PARAM, requestParameters.getFirst(EVENT_ID_PARAM));
    }

    /**
     * Get the techPack tables or the aggregation tables based on the dateTimeRange and the type parameters.
     * @param dateTimeRange
     * @param timerange
     * @param type
     * @return
     */
    private TechPackTables getTechPackTablesOrViews(final FormattedDateTimeRange dateTimeRange, final String timerange,
            final String type, final String drillType) {
        if (timerange.equals(EventDataSourceType.AGGREGATED_15MIN.getValue())
                || timerange.equals(EventDataSourceType.AGGREGATED_DAY.getValue())) {
            if (TYPE_BSC.equals(drillType)) {
                aggregationViews.put(type, new MssAggregationTableInfo(BSC_AGGR, EventDataSourceType.AGGREGATED_15MIN,
                        EventDataSourceType.AGGREGATED_DAY));
                return getAggregationTables(type, timerange, getTechPacksApplicableForType());
            } else if (TYPE_MSC.equals(drillType)) {
                aggregationViews.put(type, new MssAggregationTableInfo(MSC_AGGR, EventDataSourceType.AGGREGATED_15MIN,
                        EventDataSourceType.AGGREGATED_DAY));
                return getAggregationTables(type, timerange, getTechPacksApplicableForType());
            }
        }
        return getRawTables(type, dateTimeRange, getTechPacksApplicableForType());
    }

    /**
     * If a particular type is restricted to only certain tech pack(s) (eg BSC
     * is only applicable for the EVENT_E_MSS tech pack) then this method
     * returns only that tech pack(s) Otherwise just returns complete list of
     * tech packs
     * 
     * @return
     */
    private List<String> getTechPacksApplicableForType() {
        final List<String> listOfApplicableTechPacks = new ArrayList<String>();
        listOfApplicableTechPacks.add(EVENT_E_MSS_TPNAME);
        return listOfApplicableTechPacks;
    }
}
