/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2014
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
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.*;

import javax.ejb.EJB;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.common.*;
import com.ericsson.eniq.events.server.common.tablesandviews.*;
import com.ericsson.eniq.events.server.logging.ServicesLogger;
import com.ericsson.eniq.events.server.resources.BaseResource;
import com.ericsson.eniq.events.server.services.impl.TechPackCXCMappingService;
import com.ericsson.eniq.events.server.utils.*;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public abstract class MssBaseResource extends BaseResource {

    @EJB
    private RMIEngineUtils rmiEngineUtils;

    @EJB
    protected TechPackCXCMappingService techPackCXCMappingService;

    /**
     * Pull out the time/date parameters from the URL parameters in requestParameters, and pass these along to DateTimeRange to be converted into a
     * FormattedDateTimeRange.
     * 
     * @param requestParameters the request parameters
     * @return the formatted date time range
     * @param isCSV isCSV param is true when user click on export to CSV
     */
    private FormattedDateTimeRange getFormattedDateTimeRangeWithMSSOffsets(final MultivaluedMap<String, String> requestParameters) {

        boolean isCSV = false;
        if (isMediaTypeApplicationCSV()) {
            isCSV = true;
        }
        final FormattedDateTimeRange dateTimeRange = DateTimeRange.getFormattedDateTimeRange(requestParameters.getFirst(KEY_PARAM),
                requestParameters.getFirst(TIME_QUERY_PARAM), requestParameters.getFirst(TIME_FROM_QUERY_PARAM),
                requestParameters.getFirst(TIME_TO_QUERY_PARAM), requestParameters.getFirst(DATE_FROM_QUERY_PARAM),
                requestParameters.getFirst(DATE_TO_QUERY_PARAM), requestParameters.getFirst(DATA_TIME_FROM_QUERY_PARAM),
                requestParameters.getFirst(DATA_TIME_TO_QUERY_PARAM), requestParameters.getFirst(TZ_OFFSET),
                applicationConfigManager.getMSSTimeDelayOneMinuteData(), applicationConfigManager.getMSSTimeDelayFifteenMinuteData(),
                applicationConfigManager.getMSSTimeDelayDayData(), isCSV);
        return dateTimeRange;
    }

    /**
     * This method if for testing purposes
     */
    @Override
    protected FormattedDateTimeRange getFormattedDateTimeRange(final MultivaluedMap<String, String> requestParameters, final List<String> techPacks) {
        return getFormattedDateTimeRangeWithMSSOffsets(requestParameters);
    }

    /**
     * Currently it has been used for timeout issues in Ranking(#MultipleRankingResource) and Event Analysis(#EventAnalysisResource)
     * 
     * @param time range
     * @param key to differentiate which RAW tables we need (e.g. error, success)
     * @param templateKey key passed to template eg rawAllErrTables
     * 
     * @return the raw table names
     */

    protected List<String> getRAWTablesUsingQuery(final FormattedDateTimeRange newDateTimeRange, final String key, final String templateKey) {
        ServicesLogger.warn(MssBaseResource.class.getName(), "getRAWTablesUsingQuery", E_RMI_FAILURE);
        final String getRawTables = "GET_MSS_RAW_TABLES";
        final String drillType = null;
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        final Map<String, Object> templateParametersForRawTables = new HashMap<String, Object>();
        templateParametersForRawTables.put(KEY_PARAM, key);
        templateParametersForRawTables.put(IS_MSS_VIEW, Boolean.toString(isMSSTable(templateKey)));
        templateParametersForRawTables.put(IS_COMBINED_VIEW, Boolean.toString(isGetAllTables(templateKey)));
        final String templateName = getTemplate(getRawTables, requestParameters, drillType);
        final String query = this.templateUtils.getQueryFromTemplate(templateName, templateParametersForRawTables);
        List<String> rawtables = dataService.getRawTables(query, this.queryUtils.mapDateParameters(newDateTimeRange));

        if (rawtables == null || rawtables.isEmpty()) {
            templateParametersForRawTables.remove(DATE_FROM);
            templateParametersForRawTables.remove(DATE_TO);
            templateParametersForRawTables.put(TO_SELECT_LATEST_TABLE, true);
            final String queryForLatestTable = this.templateUtils.getQueryFromTemplate(getTemplate(getRawTables, requestParameters, drillType),
                    templateParametersForRawTables);
            rawtables = dataService.getRawTables(queryForLatestTable, null);
        }
        // select a random table so that query can run and return empty result
        // set
        if (rawtables == null || rawtables.isEmpty()) {
            templateParametersForRawTables.remove(DATE_FROM);
            templateParametersForRawTables.remove(DATE_TO);
            templateParametersForRawTables.put(TO_SELECT_LATEST_TABLE, false);
            templateParametersForRawTables.put(TO_SELECT_RANDOM_TABLE, true);
            final String queryForAnyTable = this.templateUtils.getQueryFromTemplate(getTemplate(getRawTables, requestParameters, drillType),
                    templateParametersForRawTables);
            rawtables = dataService.getRawTables(queryForAnyTable, null);
        }
        return rawtables;
    }

    /**
     * 
     * Checks if is get all tables.
     * 
     * @param templateKey the template key
     * @return true, if checks if is get all tables
     */
    private boolean isGetAllTables(final String templateKey) {
        return templateKey.toUpperCase().contains("ALL");
    }

    /**
     * 
     * @param key key passed to template eg rawAllErrTables
     * @return
     */
    protected List<String> getLatestTablesFromEngine(final String key) {
        List<String> rawtables = new ArrayList<String>();
        rawtables = this.rmiEngineUtils.getLatestTableNamesForMSSRawEvents(key);
        return rawtables;
    }

    protected List<String> getTablesFromEngine(final FormattedDateTimeRange newDateTimeRange, final String key, final String templateKey)
            throws ParseException {
        List<String> rawtables = new ArrayList<String>();
        if (isMSSTable(templateKey)) {
            rawtables = this.rmiEngineUtils.getTableNamesForMSS(
                    new Timestamp(DateTimeRange.getUTCDateTimeWithSeconds(newDateTimeRange.getStartDateTime()).getTime()), new Timestamp(
                            DateTimeRange.getUTCDateTimeWithSeconds(newDateTimeRange.getEndDateTime()).getTime()), key);
        }
        return rawtables;
    }

    /**
     * return true if template key for table name eg rawMssErrTables contains the text mss (not case sensitive)
     * 
     * @param templateKey key passed to template eg rawAllErrTables
     * @return true if templateKey indicates an mss table, false otherwise
     */
    private boolean isMSSTable(final String templateKey) {
        return templateKey.toUpperCase().contains("MSS");
    }

    @Override
    protected String getTemplateKeyMatchingTechPack(final String techPack) {
        if (techPack.equals(EVENT_E_MSS_TPNAME)) {
            return RAW_MSS_TABLES;
        }
        return null;
    }

    @Override
    protected boolean updateTemplateWithRAWTables(final Map<String, Object> templateParameters, final FormattedDateTimeRange dateTimeRange,
                                                  final String key, final String... templateKeys) {

        final List<String> totalRawtables = new ArrayList<String>();

        for (final String templateKey : templateKeys) {
            final List<String> rawtables = getRAWTables(dateTimeRange, key, templateKey);
            totalRawtables.addAll(rawtables);
            templateParameters.put(templateKey, rawtables);
        }

        return !totalRawtables.isEmpty();
    }

    /**
     * Get raw tables to use in a query. This method will look up the engine or query the time range views to determine which raw tables are
     * applicable for this time range. Depends on the sub resource sub class populating the aggregationViews map with the correct information for the
     * query/resource
     * 
     * @param type one of TYPE_APN, TYPE_SGSN etc
     * @param dateTimeRange
     * @param listOfTechPacks list of the tech packs that should be considered for this request ie EVENT_E_SGEH, EVENT_E_LTE or both
     * 
     */
    @Override
    protected TechPackTables getRawTables(final String type, final FormattedDateTimeRange dateTimeRange, final List<String> listOfTechPacks) {
        final TechPackTables techPackTables = new TechPackTables(TableType.RAW);
        for (final String techPackName : listOfTechPacks) {
            if (isTechPackApplicableForType(type, techPackName) && isTechPackInstalled(techPackName)) {
                final TechPack techPack = new TechPack(techPackName, TableType.RAW, getMatchingDIMTechpack(techPackName));
                final List<String> errTables = getRAWTables(dateTimeRange, KEY_TYPE_ERR, getTemplateKeyMatchingTechPack(techPackName));
                if (errTables.size() > 0) {
                    techPack.setErrRawTables(errTables);
                }
                final List<String> droppedTables = getRAWTables(dateTimeRange, KEY_TYPE_DROP_CALL, getTemplateKeyMatchingTechPack(techPackName));
                if (droppedTables.size() > 0) {
                    techPack.setErrRawTables(KEY_TYPE_DROP_CALL, droppedTables);
                }
                final List<String> sucTables = getRAWTables(dateTimeRange, KEY_TYPE_SUC, getTemplateKeyMatchingTechPack(techPackName));
                if (sucTables.size() > 0) {
                    techPack.setSucRawTables(sucTables);
                }
                final List<String> locServiceSucTables = getRAWTables(dateTimeRange, KEY_TYPE_LOC_SERVICE_SUC,
                        getTemplateKeyMatchingTechPack(techPackName));
                if (locServiceSucTables.size() > 0) {
                    techPack.setSucRawTables(KEY_TYPE_LOC_SERVICE_SUC, locServiceSucTables);
                }
                final List<String> locServiceErrTables = getRAWTables(dateTimeRange, KEY_TYPE_LOC_SERVICE_ERR,
                        getTemplateKeyMatchingTechPack(techPackName));
                if (locServiceErrTables.size() > 0) {
                    techPack.setErrRawTables(KEY_TYPE_LOC_SERVICE_ERR, locServiceErrTables);
                }
                final List<String> smsSucTables = getRAWTables(dateTimeRange, KEY_TYPE_SMS_SUC, getTemplateKeyMatchingTechPack(techPackName));
                if (smsSucTables.size() > 0) {
                    techPack.setSucRawTables(KEY_TYPE_SMS_SUC, smsSucTables);
                }
                final List<String> smsErrTables = getRAWTables(dateTimeRange, KEY_TYPE_SMS_ERR, getTemplateKeyMatchingTechPack(techPackName));
                if (smsErrTables.size() > 0) {
                    techPack.setErrRawTables(KEY_TYPE_SMS_ERR, smsErrTables);
                }
                if (techPack.hasAnyTables()) {
                    techPackTables.addTechPack(techPack);
                }
            }
        }

        return techPackTables;
    }

    @Override
    protected TechPackTables getRawTables(final String type, final FormattedDateTimeRange dateTimeRange) {
        return getRawTables(type, dateTimeRange, TechPackData.completeTechPackListForMss);
    }

    /**
     * This function is used for getting the raw tables which could replace a raw view for a particular time range The function first uses the RMI
     * engine, if it fails then it uses the SQL query
     * 
     * Currently it has been used for timeout issues in Ranking(#MultipleRankingResource) and Event Analysis(#EventAnalysisResource).
     * 
     * @param newDateTimeRange the new date time range
     * @param key to differentiate which RAW tables we need (e.g. error, success)
     * @param templateKey key passed to template eg rawMssTables
     * @return the raw table names
     */
    @Override
    protected List<String> getRAWTables(final FormattedDateTimeRange newDateTimeRange, final String key, final String templateKey) {

        List<String> rawtables = null;
        try {
            rawtables = getTablesFromEngine(newDateTimeRange, key, templateKey);
        } catch (final ParseException e) {
            ServicesLogger.warn(BaseResource.class.getName(), "getRAWTables",
                    "Exception thrown while trying to parse the date in order to get the table names from the engine cache. " + e.getMessage());
        }
        if (rawtables == null || rawtables.isEmpty()) {
            rawtables = getLatestTablesFromEngine(key);
            if (rawtables == null || rawtables.isEmpty()) {
                // if RMI fails then use SQL query to get the table names and
                // log the
                // event
                rawtables = getRAWTablesUsingQuery(newDateTimeRange, key, templateKey);
            }
        }
        return rawtables;
    }

    /**
     * Gets the formatted date time range without offsets.
     * 
     * @param requestParameters the request parameters
     * @return the formatted date time range
     */
    protected FormattedDateTimeRange getAndCheckFormattedDateTimeRange(final MultivaluedMap<String, String> requestParameters) {
        boolean isCSV = false;
        if (isMediaTypeApplicationCSV()) {
            isCSV = true;
        }
        final FormattedDateTimeRange timerange = DateTimeRange.getFormattedDateTimeRangeWithOutOffset(requestParameters.getFirst(TIME_QUERY_PARAM),
                requestParameters.getFirst(TIME_FROM_QUERY_PARAM), requestParameters.getFirst(TIME_TO_QUERY_PARAM),
                requestParameters.getFirst(DATE_FROM_QUERY_PARAM), requestParameters.getFirst(DATE_TO_QUERY_PARAM),
                requestParameters.getFirst(DATA_TIME_FROM_QUERY_PARAM), requestParameters.getFirst(DATA_TIME_TO_QUERY_PARAM),
                requestParameters.getFirst(TZ_OFFSET), isCSV);

        return timerange;
    }

    /**
     * This method sets up the appropriate headers etc for and executes streaming the csv data into the response using hashId.
     * 
     * @param requestParameters the request parameters
     * @param tzOffset the tz offset
     * @param timeColumn the time column
     * @param query the query
     * @param newDateTimeRange the new date time range
     */
    protected void streamDataAsCSV(final MultivaluedMap<String, String> requestParameters, final String tzOffset,
                                   final List<Integer> timeColumnIndexes, final String query, final FormattedDateTimeRange newDateTimeRange) {
        response.setContentType("application/csv");
        response.setHeader("Content-disposition", "attachment; filename=export.csv");
        try {
            this.streamingDataService.streamDataAsCsv(query, this.queryUtils.mapRequestParametersForHashId(requestParameters, newDateTimeRange),
                    timeColumnIndexes, tzOffset, getLoadBalancingPolicy(requestParameters), response.getOutputStream());
        } catch (final IOException e) {
            ServicesLogger.error(getClass().getName(), "streamDataAsCSV", e);
        }
    }

    /**
     * This method sets up the appropriate headers etc for and executes streaming the csv data into the response using hashId.
     * 
     * @param requestParameters the request parameters
     * @param tzOffset the tz offset
     * @param timeColumn the time column
     * @param queries list of queries to be executed
     * @param newDateTimeRange the new date time range
     */
    protected void streamDataAsCSV(final MultivaluedMap<String, String> requestParameters, final String tzOffset,
                                   final List<Integer> timeColumnIndexes, final List<String> queries, final FormattedDateTimeRange newDateTimeRange) {
        response.setContentType("application/csv");
        response.setHeader("Content-disposition", "attachment; filename=export.csv");
        try {
            this.streamingDataService.streamDataAsCsv(queries, this.queryUtils.mapRequestParametersForHashId(requestParameters, newDateTimeRange),
                    timeColumnIndexes, tzOffset, getLoadBalancingPolicy(requestParameters), response.getOutputStream());
        } catch (final IOException e) {
            ServicesLogger.error(getClass().getName(), "streamDataAsCSV", e);
        }
    }

    /**
     * Get the time range for the request - the method getEventDataSourceTypeForGridAndNonTimeSeriesChartWithoutOneMinAggregated () in QueryUtils is
     * used to determine which time range applies for the given dateTimeRange
     * 
     * @param dateTimeRange
     * @return
     */
    protected String getTimeRange(final FormattedDateTimeRange dateTimeRange) {
        return queryUtils.getEventDataSourceType(dateTimeRange).getValue();

    }

    @Override
    protected TechPackTables getAggregationTables(final String type, final String timerange, final List<String> listOfTechPacks) {
        final String time = ApplicationConstants.returnAggregateViewType(timerange);
        final AggregationTableInfo aggrTableInfo = aggregationViews.get(type);
        // It is not a good style to use instanceof, but currently it is used
        // for MSS
        // services supporting Voice, SMS and LOC_SERVICE
        if (aggrTableInfo instanceof MssAggregationTableInfo) {
            final MssAggregationTableInfo mssAggrTableInfo = (MssAggregationTableInfo) aggrTableInfo;
            return getAggregationTables(mssAggrTableInfo.getAggregationViewVoice(), mssAggrTableInfo.getAggregationViewSMS(),
                    mssAggrTableInfo.getAggregationViewLosService(), time, type, listOfTechPacks);
        }
        return getAggregationTables(aggrTableInfo.getAggregationView(), null, null, time, type, listOfTechPacks);
    }

    /**
     * getAggregationTables is used to getAggregationView for MSS
     * 
     * @param aggregationViewForVoice aggregation view name for voice
     * @param aggregationViewForSMS aggregation view name for sms
     * @param aggregationViewForLocService aggregation view name for loc_service
     * @param time
     * @param type
     * @param listOfTechPacks
     * @return
     */
    private TechPackTables getAggregationTables(final String aggregationViewForVoice, final String aggregationViewForSMS,
                                                final String aggregationViewForLocService, final String time, final String type,
                                                final List<String> listOfTechPacks) {
        final TechPackTables techPackTables = new TechPackTables(TableType.AGGREGATION);
        for (final String techPackName : listOfTechPacks) {
            if (isTechPackApplicableForType(type, techPackName) && isTechPackInstalled(techPackName)) {
                final TechPack techPack = new TechPack(techPackName, TableType.AGGREGATION, getMatchingDIMTechpack(techPackName));
                final String errVoiceTable = techPackName + UNDERSCORE + VOICE + UNDERSCORE + aggregationViewForVoice + UNDERSCORE + KEY_TYPE_ERR
                        + time;
                techPack.setErrAggregationView(errVoiceTable);

                final String dropVoiceTable = techPackName + UNDERSCORE + VOICE + UNDERSCORE + aggregationViewForVoice + UNDERSCORE
                        + KEY_TYPE_DROP_CALL + time;
                techPack.setErrAggregationView(KEY_TYPE_DROP_CALL, dropVoiceTable);

                final String sucVoiceTable = techPackName + UNDERSCORE + VOICE + UNDERSCORE + aggregationViewForVoice + UNDERSCORE + KEY_TYPE_SUC
                        + time;
                techPack.setSucAggregationView(sucVoiceTable);

                if (aggregationViewForSMS != null) {
                    final String errSMSTable = techPackName + UNDERSCORE + SMS + UNDERSCORE + aggregationViewForSMS + UNDERSCORE + KEY_TYPE_ERR
                            + time;
                    techPack.setErrAggregationView(KEY_TYPE_SMS_ERR, errSMSTable);

                    final String sucSMSTable = techPackName + UNDERSCORE + SMS + UNDERSCORE + aggregationViewForSMS + UNDERSCORE + KEY_TYPE_SUC
                            + time;
                    techPack.setSucAggregationView(KEY_TYPE_SMS_SUC, sucSMSTable);
                }
                if (aggregationViewForLocService != null) {
                    final String errLocServiceTable = techPackName + UNDERSCORE + LOC_SERVICE + UNDERSCORE + aggregationViewForLocService
                            + UNDERSCORE + KEY_TYPE_ERR + time;
                    techPack.setErrAggregationView(KEY_TYPE_LOC_SERVICE_ERR, errLocServiceTable);

                    final String sucLocServiceTable = techPackName + UNDERSCORE + LOC_SERVICE + UNDERSCORE + aggregationViewForLocService
                            + UNDERSCORE + KEY_TYPE_SUC + time;
                    techPack.setSucAggregationView(KEY_TYPE_LOC_SERVICE_SUC, sucLocServiceTable);
                }

                techPackTables.addTechPack(techPack);
            }
        }
        return techPackTables;
    }

    /**
     * Update the type for MSS. Add MSS in front of the original type parameters.
     * 
     * @param type
     * @param requestParameters
     */
    protected void updateMssTypeParameter(final String type, final MultivaluedMap<String, String> requestParameters) {
        String newType = type;
        if (!TYPE_INTERNAL_CAUSE_CODE.equals(type)) {
            newType = MSS + UNDERSCORE + type;
            requestParameters.putSingle(TYPE_PARAM, newType);
        } else {
            requestParameters.putSingle(TYPE_PARAM, type);
        }
    }

    // This method is similar to BaseReource.java:getTemplate. Only use MSS_ +
    // type as its type
    protected String getTemplateForMss(final String pathName, final MultivaluedMap<String, String> requestParameters, final String drillType,
                                       final String timerange) {
        boolean isGroup = false;
        String type = null;
        boolean hasEventID = false;
        String key = null;
        if (requestParameters.containsKey(GROUP_NAME_PARAM)) {
            isGroup = true;
        }
        if (requestParameters.containsKey(TYPE_PARAM)) {
            type = MSS + UNDERSCORE + requestParameters.getFirst(TYPE_PARAM);
        }
        if (requestParameters.containsKey(EVENT_ID_PARAM)) {
            hasEventID = true;
        }
        if (requestParameters.containsKey(KEY_PARAM)) {
            key = requestParameters.getFirst(KEY_PARAM);
        }
        final String[] keys = { pathName, String.valueOf(type), String.valueOf(isGroup), String.valueOf(drillType), String.valueOf(hasEventID),
                String.valueOf(key), String.valueOf(returnAggregateViewType(timerange)) };
        final MultiKey multiKey = new MultiKey(keys);
        return this.templateMappingEngine.getTemplate(multiKey);
    }

    /**
     * This method is used to determine the time column indexes based on the eventID
     * 
     * @param templateParameters request specific parameters
     * @return list of time column indexes to use for this request
     */
    protected List<Integer> getEventTypeSpecificTimeColumns(final String evntID) {
        List<Integer> timeColumnIndexes = new ArrayList<Integer>();
        timeColumnIndexes.add(MSS_MS_ORIGINATING_TERMINATING_EVENT_TIME_COLUMN_INDEX);
        timeColumnIndexes.add(MSS_MS_ORIGINATING_TERMINATING_SEIZURE_TIME_COLUMN_INDEX);
        if (evntID != null && MSS_CALL_FORWARDING_EVENT_ID.equals(evntID)) {
            timeColumnIndexes = new ArrayList<Integer>();
            timeColumnIndexes.add(MSS_CALL_FORWARDING_ROAMING_CALL_EVENT_TIME_COLUMN_INDEX);
            timeColumnIndexes.add(MSS_CALLFORWARDING_SEIZURE_TIME_COLUMN_INDEX);
        } else if (evntID != null && MSS_ROAMING_CALL_EVENT_ID.equals(evntID)) {
            timeColumnIndexes = new ArrayList<Integer>();
            timeColumnIndexes.add(MSS_CALL_FORWARDING_ROAMING_CALL_EVENT_TIME_COLUMN_INDEX);
            timeColumnIndexes.add(MSS_ROAMING_CALL_SEIZURE_TIME_COLUMN_INDEX);
        } else if (evntID != null && isMssLocationServiceEvent(evntID)) {
            timeColumnIndexes = new ArrayList<Integer>();
            timeColumnIndexes.add(MSS_MS_ORIGINATING_TERMINATING_EVENT_TIME_COLUMN_INDEX);
        } else if (evntID != null && MSS_SMS_MS_ORIGINATING_EVENT_ID.equals(evntID)) {
            timeColumnIndexes = new ArrayList<Integer>();
            timeColumnIndexes.add(MSS_MS_ORIGINATING_TERMINATING_EVENT_TIME_COLUMN_INDEX);
            timeColumnIndexes.add(MSS_SMS_ORIGINATING_EVENT_TIME_COLUMN_INDEX);
        } else if (evntID != null && MSS_SMS_MS_TERMINATING_EVENT_ID.equals(evntID)) {
            timeColumnIndexes = new ArrayList<Integer>();
            timeColumnIndexes.add(MSS_MS_ORIGINATING_TERMINATING_EVENT_TIME_COLUMN_INDEX);
            timeColumnIndexes.add(MSS_SMS_DELIVERY_EVENT_TIME_COLUMN_INDEX);
        }

        return timeColumnIndexes;
    }

    /**
     * Gets the date time range of chart and summary grid with MSS offsets.
     * 
     * @param dateTimeRange - FormattedDateTimeRange
     * @param viewName - aggregation view name
     * @return the formatted dateTime range of chart and summary grid
     * @throws WebApplicationException the parse exception
     */
    @Override
    public FormattedDateTimeRange getDateTimeRangeOfChartAndSummaryGrid(final FormattedDateTimeRange dateTimeRange, final String viewName,
                                                                        final List<String> techPacks) throws WebApplicationException {
        FormattedDateTimeRange newDateTimeRange = null;
        if (viewName.equals(EventDataSourceType.AGGREGATED_15MIN.getValue())) {
            newDateTimeRange = DateTimeRange.getFormattedDateTimeRange(
                    DateTimeRange.formattedDateTimeAgainst15MinsTable(dateTimeRange.getStartDateTime(), dateTimeRange.getMinutesOfStartDateTime()),
                    DateTimeRange.formattedDateTimeAgainst15MinsTable(dateTimeRange.getEndDateTime(), dateTimeRange.getMinutesOfEndDateTime()),
                    applicationConfigManager.getMSSTimeDelayOneMinuteData(), applicationConfigManager.getMSSTimeDelayFifteenMinuteData(),
                    applicationConfigManager.getMSSTimeDelayDayData());
        } else if (viewName.equals(EventDataSourceType.AGGREGATED_DAY.getValue())) {
            newDateTimeRange = DateTimeRange.getFormattedDateTimeRange(
                    DateTimeRange.formattedDateTimeAgainstDayTable(dateTimeRange.getStartDateTime(), dateTimeRange.getMinutesOfStartDateTime()),
                    DateTimeRange.formattedDateTimeAgainstDayTable(dateTimeRange.getEndDateTime(), dateTimeRange.getMinutesOfEndDateTime()),
                    applicationConfigManager.getMSSTimeDelayOneMinuteData(), applicationConfigManager.getMSSTimeDelayFifteenMinuteData(),
                    applicationConfigManager.getMSSTimeDelayDayData());
        } else {
            newDateTimeRange = dateTimeRange;
        }
        return newDateTimeRange;
    }

    /**
     * Gets the formatted date time range with MSS offsets.
     * 
     * @param requestParameters the request parameters
     * @return the formatted date time range
     */
    @Override
    protected FormattedDateTimeRange getAndCheckFormattedDateTimeRangeForDailyAggregation(final MultivaluedMap<String, String> requestParameters,
                                                                                          final List<String> techPacks) {
        FormattedDateTimeRange timerange = getFormattedDateTimeRange(requestParameters, techPacks);
        if (requestParameters.containsKey(TIME_QUERY_PARAM)) {
            if ((queryUtils.getEventDataSourceType(timerange).equals(EventDataSourceType.AGGREGATED_DAY.getValue()))) {
                timerange = DateTimeRange.getDailyAggregationTimeRangebyLocalTime(requestParameters.getFirst(TIME_QUERY_PARAM),
                        applicationConfigManager.getMSSTimeDelayOneMinuteData(), applicationConfigManager.getMSSTimeDelayFifteenMinuteData(),
                        applicationConfigManager.getMSSTimeDelayDayData());
            }
        }
        return timerange;
    }

    /**
     * This method is used to determine whether to exclude events containing TAC that are part of exclusive TAC group
     * 
     * @param requestParameters url parameters
     * @return true if the table view is raw else false
     */
    protected boolean isExcludedTacOrGroup(final MultivaluedMap<String, String> requestParameters) {
        final String type = requestParameters.getFirst(TYPE_PARAM);
        final String tac = requestParameters.getFirst(TAC_PARAM);
        final String groupName = requestParameters.getFirst(GROUP_NAME_PARAM);
        if (!TYPE_TAC.equals(type)) {
            return false;
        } else if (StringUtils.isNotBlank(groupName) && isExclusiveTacGroup(groupName)) {
            return true;
        } else if (StringUtils.isNotBlank(tac) && isTacInExclusiveTacGroup(tac)) {
            return true;
        }
        return false;
    }

    public void setRMIEngineUtils(final RMIEngineUtils rmiEngineUtils) {
        this.rmiEngineUtils = rmiEngineUtils;

    }

    private boolean isTechPackInstalled(final String techPackName) {
        final List<String> cxcLicensesForTechPack = techPackCXCMappingService.getTechPackCXCNumbers(techPackName);
        return !cxcLicensesForTechPack.isEmpty();
    }

    public void setTechPackCXCMappingService(final TechPackCXCMappingService techPackCXCMappingService) {
        this.techPackCXCMappingService = techPackCXCMappingService;
    }
}
