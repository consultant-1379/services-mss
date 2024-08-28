package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.errSucTotal;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.QueryConstructorConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;
import javax.ejb.Stateless;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.common.GroupHashId;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.queryconstructor.QueryConstructorUtils;

/**
 * Class used to create query for all node and node groups for KEY= SUC,ERR,SUM,TOTAL
 * @author echchik
 *
 */

@Stateless
public class MssEventAnalysisErrSucTotalQueryCreator {

    private static List<String> filterColums;

    private static List<String> locationServiceFilterColums;

    private static List<String> smsFilterColums;

    private static List<String> subscriberDetailsFilterColums;

    /*
     * Map to hold the parameters used to construct the query
     */
    private Map<String, Object> evntAnalysisParameters;;

    /*
     * This object provide raw tables for blocked, dropped and success
     */
    private TechPackTables techPackTables;

    /*
     * This parameter represent the type for which query should be created
     */
    private String type;

    /*
     * This map provides info about the group join keys, table to use
     */
    private Map<String, GroupHashId> groups;

    /*
     * This utility is used to create query using template utilities
     */
    @EJB
    protected QueryConstructorUtils queryConsUtil;

    static {
        filterColums = new ArrayList<String>();
        locationServiceFilterColums = new ArrayList<String>();
        smsFilterColums = new ArrayList<String>();

        filterColums.addAll(RawColumnsToSelect.COLUMNS_ORDER_DESPCRITION.getRawColumns());
        //filterColums.addAll(RawColumnsToSelect.COLUMNS_ORDER_1.getRawColumns());
        filterColums.addAll(RawColumnsToSelect.COLUMNS_ORDER_1_1.getRawColumns());
        filterColums.addAll(RawColumnsToSelect.COLUMNS_ORDER_1_2.getRawColumns());
        filterColums.addAll(RawColumnsToSelect.COLUMNS_ORDER_1_3.getRawColumns());
        filterColums.addAll(RawColumnsToSelect.COLUMNS_ORDER_3_1.getRawColumns());
        filterColums.addAll(RawColumnsToSelect.COLUMNS_ORDER_3_2.getRawColumns());
        filterColums.addAll(RawColumnsToSelect.COLUMNS_ORDER_3_3.getRawColumns());
        filterColums.addAll(RawColumnsToSelect.COLUMNS_ORDER_3_4.getRawColumns());
        filterColums.addAll(RawColumnsToSelect.COLUMNS_ORDER_4.getRawColumns());

        locationServiceFilterColums.addAll(RawColumnsToSelect.LOC_SERVICE_COLUMNS_ORDER_DESPCRITION.getRawColumns());
        locationServiceFilterColums.addAll(RawColumnsToSelect.COLUMNS_ORDER_1.getRawColumns());
        locationServiceFilterColums.addAll(RawColumnsToSelect.LOC_SERVICE_COLUMNS_ORDER_3.getRawColumns());
        locationServiceFilterColums.addAll(RawColumnsToSelect.LOC_SERVICE_COLUMNS_ORDER_4.getRawColumns());

        smsFilterColums.addAll(RawColumnsToSelect.SMS_COLUMNS_ORDER_DESPCRITION.getRawColumns());
        smsFilterColums.addAll(RawColumnsToSelect.COLUMNS_ORDER_1.getRawColumns());
        smsFilterColums.addAll(RawColumnsToSelect.SMS_COLUMNS_ORDER_3.getRawColumns());
        smsFilterColums.addAll(RawColumnsToSelect.SMS_COLUMNS_ORDER_4.getRawColumns());

        subscriberDetailsFilterColums = new ArrayList<String>();
        subscriberDetailsFilterColums.addAll(RawColumnsToSelect.COLUMNS_ORDER_SUBSCRIBER_DESPCRITION.getRawColumns());
        subscriberDetailsFilterColums.addAll(RawColumnsToSelect.COLUMNS_ORDER_SUBSCRIBER_DETAILS.getRawColumns());
    }

    /**
     * This method returns the query used to fetch data based on templateParameters
     * @param templateParameters
     * @return
     */
    public List<String> createQuery(final Map<String, Object> templateParameters) {
        evntAnalysisParameters = new HashMap<String, Object>();
        evntAnalysisParameters.putAll(templateParameters);
        evntAnalysisParameters.put(USE_TAC_EXCLUSION_BOOLEAN, toUseTacExclusion());
        populateParameters();
        if (isEventIDPresent()) {
            return CreateQueryForForEvntID();
        }
        return createQueryForAllEvents();
    }

    /**
     * This method returns the query used to fetch subscriber details
     * @param templateParameters
     * @return query to use for fetching subscriber details
     */
    public List<String> createSubcriberDetailsQuery(final Map<String, Object> templateParameters) {
        evntAnalysisParameters = new HashMap<String, Object>();
        evntAnalysisParameters.putAll(templateParameters);
        evntAnalysisParameters.put(USE_TAC_EXCLUSION_BOOLEAN, toUseTacExclusion());
        populateParametersForSubscriberDetails();
        if ((!type.equals(TYPE_IMSI)) && (!type.equals(TYPE_MSISDN))) {
            return new ArrayList<String>();
        }
        evntAnalysisParameters.put(RAW_TABLE_QUERY_VAR, getSubscriberDetailsQuery(false));
        final List<String> queryList = new ArrayList<String>();
        final String query;
        if (type.equals(TYPE_MSISDN)) {
            query = queryConsUtil.getQueryFromTemplate(MSS_SUBBI_MSISDN_SUBSCRIBER_DETAILS_VOICE_TEMPLATE,
                    evntAnalysisParameters);
        } else {
            query = queryConsUtil.getQueryFromTemplate(MSS_SUBBI_SUBSCRIBER_DETAILS_VOICE_TEMPLATE,
                    evntAnalysisParameters);
        }

        queryList.add(query);
        return queryList;
    }

    /**
     * This method is used to determine to exclude TAC part of EXCLUSIVE_TAC group
     * @return false if the query is for EXCLUSIVE_TAC group or TAC part of 
     * EXCLUSIVE_TAC group else true
     */
    private boolean toUseTacExclusion() {
        final Boolean isExcluedTacOrGroup = (Boolean) evntAnalysisParameters.get(IS_EXCULDED_TAC_OR_TACGROUP);
        if (isExcluedTacOrGroup == null) {
            return true;
        }
        return !isExcluedTacOrGroup;
    }

    /**
     * This method is used to populate the common data needed by query templates to generate query
     * to fetch subscriber details 
     */
    private void populateParametersForSubscriberDetails() {
        evntAnalysisParameters.put(RAW_TABLE_COLUMN_ORDER_1_VAR,
                RawColumnsToSelect.COLUMNS_ORDER_SUBSCRIBER_DETAILS.getRawColumns());
        type = (String) evntAnalysisParameters.get(TYPE_PARAM);
        evntAnalysisParameters.put(KEY_PARAM, KEY_TYPE_ERR);
        techPackTables = (TechPackTables) evntAnalysisParameters.get(TECH_PACK_TABLES);
        evntAnalysisParameters.put(FILTER_COLUMNS_VAR, subscriberDetailsFilterColums);
        evntAnalysisParameters.put(JOIN_COLUMNS_VAR, getColumnsJoinColumns());
        evntAnalysisParameters.put(EXTRA_TABLE_JOIN_COLUMNS_VAR, new ArrayList<String>());
        evntAnalysisParameters.put(EXTRA_TABLE_VAR, "");
    }

    /**
     * This method is used to fetch event id specific events passed as part of URL
     * @return query that returns specific events data
     */
    private List<String> CreateQueryForForEvntID() {
        final List<String> queryList = new ArrayList<String>();
        evntAnalysisParameters.put(RAW_TABLE_QUERY_VAR, getRawTableSubQuery());
        final String query = queryConsUtil.getQueryFromTemplate(MSS_ERR_SUC_TOTAL_EVNT_ANALYSIS_TOTAL_VOICE_TEMPLATE,
                evntAnalysisParameters);
        queryList.add(query);
        return queryList;
    }

    /**
     * This method is used to fetch raw view sub query for different schemas based on event id
     * @return query that returns raw view sub query
     */
    private String getRawTableSubQuery() {
        final String evntID = (String) evntAnalysisParameters.get(EVENT_ID_PARAM);
        if (isMssVoiceEvent(evntID)) {
            populateVoiceSpecificParameters();
            populateCallForwardSpecificParameters(evntID);
            return createRawTableSubQuery(false);
        } else if (isMssLocationServiceEvent(evntID)) {
            populateLocationServiceSpecificParameters();
            return createRawTableSubQueryForLocService();
        } else if (isMssSMSEvent(evntID)) {
            populateSmsSpecificParameters();
            return createRawTableSubQueryForSms();
        }
        return null;
    }

    private void populateCallForwardSpecificParameters(final String eventId) {
        if (MSS_CALL_FORWARDING_EVENT_ID.equals(eventId) || MSS_ROAMING_CALL_EVENT_ID.equals(eventId)) {
            evntAnalysisParameters.put(IS_MSS_CALLFORWARD_REPORT_VAR, true);
        } else {
            evntAnalysisParameters.put(IS_MSS_CALLFORWARD_REPORT_VAR, false);
        }
    }

    /**
     * This method is used to create query for all event types.
     * This is done as different events have different topology data.Hence based on event id the
     * topology is joined and add empty space in case no topology data is present
     * @return query that returns all events data
     */
    private List<String> createQueryForAllEvents() {

        addEventSpecificQueryToMap(MSS_MS_ORIGINATING_EVENT_ID, MS_ORIGINATING_TABLE_VAR);
        addEventSpecificQueryToMap(MSS_MS_TERMINATING_EVENT_ID, MS_TERMINATING_TABLE_VAR);
        addEventSpecificQueryToMap(MSS_CALL_FORWARDING_EVENT_ID, CALL_FORWARDING_TABLE_VAR);
        addEventSpecificQueryToMap(MSS_ROAMING_CALL_EVENT_ID, ROAMING_CALL_TABLE_VAR);

        final List<String> queryList = new ArrayList<String>();
        final String query = queryConsUtil.getQueryFromTemplate(
                MSS_ERR_SUC_TOTAL_ALL_EVENTS_EVNT_ANALYSIS_VOICE_TEMPLATE, evntAnalysisParameters);
        queryList.add(query);
        return queryList;
    }

    /**
     * This method will create subquery that represents each event type
     * @param eventId condition to fetch raw events 
     * @param eventIdTableVar template variable that holds sub query for the eventID
     */
    private void addEventSpecificQueryToMap(final String eventId, final String eventIdTableVar) {
        evntAnalysisParameters.put(EVENT_ID_PARAM, eventId);
        toJoinOnCellControllerInfoAndTac();
        populateVoiceSpecificParameters();
        populateCallForwardSpecificParameters(eventId);
        evntAnalysisParameters.put(RAW_TABLE_QUERY_VAR, createRawTableSubQuery(true));
        final String msRoamingCallEventsTable = queryConsUtil.getQueryFromTemplate(
                MSS_ERR_SUC_TOTAL_CONSTANT_COLUMNS_EVNT_ANALYSIS_VOICE_TEMPLATE, evntAnalysisParameters);
        evntAnalysisParameters.put(eventIdTableVar, msRoamingCallEventsTable);
    }

    /**
     * This method is used to create the raw view from err and suc tables from voice service schema
     * @param isConstantEventId true if to add default event id passed manually 
     *                          in this class else false if the event ID is part of URL
     * @return subquery representing raw tables
     */
    private String createRawTableSubQuery(final boolean isConstantEventId) {
        final String key = (String) evntAnalysisParameters.get(KEY_PARAM);
        if (key.equals(KEY_TYPE_ERR)) {
            return getErrKeyQuery(isConstantEventId);
        } else if (key.equals(KEY_TYPE_SUC)) {
            return getSucKeyQuery(isConstantEventId);
        } else {
            return getTotalQuery(isConstantEventId);
        }
    }

    /**
     * This method is used to create the raw view from err and suc tables from location service schema
     * @return subquery representing raw tables
     */
    private String createRawTableSubQueryForLocService() {
        final String key = (String) evntAnalysisParameters.get(KEY_PARAM);
        if (key.equals(KEY_TYPE_ERR)) {
            return getLocServiceErrorQuery();
        } else if (key.equals(KEY_TYPE_SUC)) {
            return getLocServiceSuccessTable();
        } else {
            return getLocServiceTotalQuery();
        }
    }

    /**
     * This method is used to create the raw view from err and suc tables from SMS schema
     * @return subquery representing raw tables
     */
    private String createRawTableSubQueryForSms() {
        final String key = (String) evntAnalysisParameters.get(KEY_PARAM);
        if (key.equals(KEY_TYPE_ERR)) {
            return getSmsErrorQuery();
        } else if (key.equals(KEY_TYPE_SUC)) {
            return getSmsSuccessTable();
        } else {
            return getSmsTotalQuery();
        }
    }

    /**
     * This method is used to populate the common data needed by query templates to generate query
     * for all node and node groups 
     */
    @SuppressWarnings("unchecked")
    private void populateParameters() {
        evntAnalysisParameters.put(IS_MSS_VOICE_REPORT_VAR, false);
        evntAnalysisParameters.put(IS_MSS_LOC_SERVICE_REPORT_VAR, false);
        evntAnalysisParameters.put(IS_MSS_SMS_REPORT_VAR, false);
        // evntAnalysisParameters.put(RAW_TABLE_COLUMN_ORDER_1_VAR, RawColumnsToSelect.COLUMNS_ORDER_1.getRawColumns());
        evntAnalysisParameters.put(RAW_TABLE_COLUMN_ORDER_2_VAR, RawColumnsToSelect.COLUMNS_ORDER_2.getRawColumns());
        type = (String) evntAnalysisParameters.get(TYPE_PARAM);
        techPackTables = (TechPackTables) evntAnalysisParameters.get(TECH_PACK_TABLES);
        groups = (Map<String, GroupHashId>) evntAnalysisParameters.get(GROUP_DEFINITIONS);
        evntAnalysisParameters.put(JOIN_COLUMNS_VAR, getColumnsJoinColumns());
        evntAnalysisParameters.put(EXTRA_TABLE_JOIN_COLUMNS_VAR, new ArrayList<String>());
        evntAnalysisParameters.put(EXTRA_TABLE_VAR, "");
        toJoinOnCellControllerInfoAndTac();
        if (evntAnalysisParameters.containsKey(GROUP_NAME_KEY)) {
            populateParametersForGroup();
        }
        if (type.equals(TYPE_MAN)) {
            final List<String> manufExtraJoinColumns = new ArrayList<String>();
            manufExtraJoinColumns.add(TAC_PARAM.toUpperCase());
            evntAnalysisParameters.put(EXTRA_TABLE_JOIN_COLUMNS_VAR, manufExtraJoinColumns);
            evntAnalysisParameters.put(EXTRA_TABLE_VAR, DIM_E_SGEH_TAC);
        }
    }

    /**
     * This method populated voice related parameter necessary for voice query generation
     */
    private void populateVoiceSpecificParameters() {
        evntAnalysisParameters.put(IS_MSS_VOICE_REPORT_VAR, true);
        evntAnalysisParameters
                .put(RAW_TABLE_COLUMN_ORDER_1_1_VAR, RawColumnsToSelect.COLUMNS_ORDER_1_1.getRawColumns());
        evntAnalysisParameters
                .put(RAW_TABLE_COLUMN_ORDER_1_2_VAR, RawColumnsToSelect.COLUMNS_ORDER_1_2.getRawColumns());
        evntAnalysisParameters
                .put(RAW_TABLE_COLUMN_ORDER_1_3_VAR, RawColumnsToSelect.COLUMNS_ORDER_1_3.getRawColumns());
        evntAnalysisParameters
                .put(RAW_TABLE_COLUMN_ORDER_3_1_VAR, RawColumnsToSelect.COLUMNS_ORDER_3_1.getRawColumns());
        evntAnalysisParameters
                .put(RAW_TABLE_COLUMN_ORDER_3_2_VAR, RawColumnsToSelect.COLUMNS_ORDER_3_2.getRawColumns());
        evntAnalysisParameters
                .put(RAW_TABLE_COLUMN_ORDER_3_3_VAR, RawColumnsToSelect.COLUMNS_ORDER_3_3.getRawColumns());
        evntAnalysisParameters
                .put(RAW_TABLE_COLUMN_ORDER_3_4_VAR, RawColumnsToSelect.COLUMNS_ORDER_3_4.getRawColumns());
        evntAnalysisParameters.put(RAW_TABLE_COLUMN_ORDER_4_VAR, RawColumnsToSelect.COLUMNS_ORDER_4.getRawColumns());
        evntAnalysisParameters.put(FILTER_COLUMNS_VAR, filterColums);

    }

    /**
     * This method populated location Service related parameter necessary for 
     * location service query generation
     */
    private void populateLocationServiceSpecificParameters() {
        evntAnalysisParameters.put(IS_MSS_LOC_SERVICE_REPORT_VAR, true);
        evntAnalysisParameters.put(RAW_TABLE_COLUMN_ORDER_1_VAR, RawColumnsToSelect.COLUMNS_ORDER_1.getRawColumns());
        evntAnalysisParameters.put(RAW_TABLE_COLUMN_ORDER_3_VAR,
                RawColumnsToSelect.LOC_SERVICE_COLUMNS_ORDER_3.getRawColumns());
        evntAnalysisParameters.put(RAW_TABLE_COLUMN_ORDER_4_VAR,
                RawColumnsToSelect.LOC_SERVICE_COLUMNS_ORDER_4.getRawColumns());
        evntAnalysisParameters.put(FILTER_COLUMNS_VAR, locationServiceFilterColums);
    }

    /**
     * This method populated SMS related parameter necessary for SMS query generation
     */
    private void populateSmsSpecificParameters() {
        evntAnalysisParameters.put(IS_MSS_SMS_REPORT_VAR, true);
        evntAnalysisParameters.put(RAW_TABLE_COLUMN_ORDER_1_VAR, RawColumnsToSelect.COLUMNS_ORDER_1.getRawColumns());
        evntAnalysisParameters
                .put(RAW_TABLE_COLUMN_ORDER_3_VAR, RawColumnsToSelect.SMS_COLUMNS_ORDER_3.getRawColumns());
        evntAnalysisParameters
                .put(RAW_TABLE_COLUMN_ORDER_4_VAR, RawColumnsToSelect.SMS_COLUMNS_ORDER_4.getRawColumns());
        evntAnalysisParameters.put(FILTER_COLUMNS_VAR, smsFilterColums);
    }

    /**
     * This method is used to populate the common data needed by query templates to generate query
     * for all node groups 
     */
    private void populateParametersForGroup() {
        final String grpType = getGrpRelatedType();
        final GroupHashId grpDef = groups.get(grpType);
        final String grpNameColumn = grpDef.getGroupNameColumn();
        final List<String> joincolumnsForGrp = new ArrayList<String>();
        joincolumnsForGrp.add(grpNameColumn);
        evntAnalysisParameters.put(EXTRA_TABLE_JOIN_COLUMNS_VAR, grpDef.getGroupKeys());
        evntAnalysisParameters.put(EXTRA_TABLE_VAR, grpDef.getTableName());
        evntAnalysisParameters.put(JOIN_COLUMNS_VAR, joincolumnsForGrp);
    }

    /**
     * This method is used to determine whether a join should be done to fetch the topology data
     * for CELL,CONTROLLER,RAT,VENDOR and TAC for roaming and call forwarding event types
     */
    private void toJoinOnCellControllerInfoAndTac() {
        final String evntID = (String) evntAnalysisParameters.get(EVENT_ID_PARAM);
        evntAnalysisParameters.put(JOIN_ON_CELL_CONTROLLER_VAR, true);
        evntAnalysisParameters.put(JOIN_ON_TAC_VAR, true);
        if (!isEventIDPresent()) {
            return;
        }
        if (MSS_CALL_FORWARDING_EVENT_ID.equals(evntID)) {
            evntAnalysisParameters.put(JOIN_ON_CELL_CONTROLLER_VAR, false);
            evntAnalysisParameters.put(JOIN_ON_TAC_VAR, false);
        } else if (MSS_ROAMING_CALL_EVENT_ID.equals(evntID)) {
            evntAnalysisParameters.put(JOIN_ON_CELL_CONTROLLER_VAR, false);
        }
    }

    /**
     * This method is used to check whether event id is present in URL
     * @return true if valid event id is present else false
     */
    private boolean isEventIDPresent() {
        final String evntID = (String) evntAnalysisParameters.get(EVENT_ID_PARAM);
        if (StringUtils.isNotBlank(evntID)) {
            return true;
        }
        return false;
    }

    /**
     * This method is used to get the all tables query part to be used in the final query
     * for key type SUC
     * @return string representing success table query
     */
    private String getSucKeyQuery(final boolean isConstantEventId) {
        return getSuccessTable(isConstantEventId);
    }

    /**
     * This method is used to get the all tables voice query part to be used in the final query
     * for key type SUM or TOTAL
     * @return string representing all table query
     */
    private String getTotalQuery(final boolean isConstantEventId) {
        final StringBuilder query = new StringBuilder();
        final String blockedQueryPart = getBlockedTable(isConstantEventId);
        query.append(blockedQueryPart);
        final String unionStr = getUnionAll();
        query.append(unionStr);
        final String droppedQueryPart = getDroppedTable(isConstantEventId);
        query.append(droppedQueryPart);
        query.append(unionStr);
        final String successQueryPart = getSuccessTable(isConstantEventId);
        query.append(successQueryPart);
        return query.toString();
    }

    /**
     * This method is used to get the all tables location service query part to be used 
     * in the final query for key type SUM or TOTAL
     * @return string representing all table query
     */
    private String getLocServiceTotalQuery() {
        final StringBuilder query = new StringBuilder();
        final String errorQueryPart = getLocServiceErrorQuery();
        query.append(errorQueryPart);
        final String unionStr = getUnionAll();
        query.append(unionStr);
        final String successQueryPart = getLocServiceSuccessTable();
        query.append(successQueryPart);
        return query.toString();
    }

    /**
     * This method is used to get the all tables SMS query part to be used 
     * in the final query for key type SUM or TOTAL
     * @return string representing all table query
     */
    private String getSmsTotalQuery() {
        final StringBuilder query = new StringBuilder();
        final String errorQueryPart = getSmsErrorQuery();
        query.append(errorQueryPart);
        final String unionStr = getUnionAll();
        query.append(unionStr);
        final String successQueryPart = getSmsSuccessTable();
        query.append(successQueryPart);
        return query.toString();
    }

    /**
     * This method is used to get the error table query part to be used in the final query
     * @return string representing error table query
     */
    private String getErrKeyQuery(final boolean isConstantEventId) {
        final StringBuilder query = new StringBuilder();
        if(evntAnalysisParameters.containsKey(FAIL_TYPE)){
           if(evntAnalysisParameters.get(FAIL_TYPE).equals(BLOCKED_VALUE)){
              final String blockedQueryPart = getBlockedTable(isConstantEventId);
              query.append(blockedQueryPart);
           }
           if(evntAnalysisParameters.get(FAIL_TYPE).equals(DROPPED_VALUE)){
               final String droppedQueryPart = getDroppedTable(isConstantEventId);
               query.append(droppedQueryPart);
           }
        }
        else{
          final String blockedQueryPart = getBlockedTable(isConstantEventId);
          query.append(blockedQueryPart);
          final String unionStr = getUnionAll();
          query.append(unionStr);
          final String droppedQueryPart = getDroppedTable(isConstantEventId);
          query.append(droppedQueryPart);
        }
        return query.toString();

    }

    /**
     * This method is used to get the Subscriber details table query part to be 
     * used in the final query
     * @return string representing subscriber details table query
     */
    private String getSubscriberDetailsQuery(final boolean isConstantEventId) {
        final StringBuilder query = new StringBuilder();
        final String unionStr = getUnionAll();
        final String blockedQueryPart = getBlockedTable(isConstantEventId);
        query.append(blockedQueryPart);
        query.append(unionStr);
        final String droppedQueryPart = getDroppedTable(isConstantEventId);
        query.append(droppedQueryPart);
        query.append(unionStr);
        final String smsQueryPart = getSmsErrorQuery();
        query.append(smsQueryPart);
        query.append(unionStr);
        final String locServiceQueryPart = getLocServiceErrorQuery();
        query.append(locServiceQueryPart);
        return query.toString();

    }

    /**
     * This method is used to get the location service error table query part to be used 
     * in the final query
     * @return string representing error table query
     */
    private String getLocServiceErrorQuery() {
        evntAnalysisParameters.put(TABLES_VAR, techPackTables.getErrTables(KEY_TYPE_LOC_SERVICE_ERR));
        evntAnalysisParameters.put(EVENT_RESULT_PARAM, EVENT_RESULT_ERROR);
        return queryConsUtil.getQueryFromTemplate(MSS_ERR_SUC_TOTAL_EVNT_ANALYSIS_VOICE_TEMPLATE,
                evntAnalysisParameters);
    }

    /**
     * This method is used to get the SMS error table query part to be used 
     * in the final query
     * @return string representing error table query
     */
    private String getSmsErrorQuery() {
        evntAnalysisParameters.put(TABLES_VAR, techPackTables.getErrTables(KEY_TYPE_SMS_ERR));
        evntAnalysisParameters.put(EVENT_RESULT_PARAM, EVENT_RESULT_ERROR);
        return queryConsUtil.getQueryFromTemplate(MSS_ERR_SUC_TOTAL_EVNT_ANALYSIS_VOICE_TEMPLATE,
                evntAnalysisParameters);
    }

    /**
     * This method is used to get the blocked table query part to be used in the error query
     * @return string representing blocked table query
     */
    private String getBlockedTable(final boolean isConstantEventID) {
        evntAnalysisParameters.put(TABLES_VAR, techPackTables.getErrTables(KEY_TYPE_ERR));
        evntAnalysisParameters.put(EVENT_RESULT_PARAM, EVENT_RESULT_BLOCKED);
        if (isConstantEventID) {
            return queryConsUtil.getQueryFromTemplate(MSS_ERR_SUC_TOTAL_CONSTANT_EVENT_ID_EVNT_ANALYSIS_VOICE_TEMPLATE,
                    evntAnalysisParameters);
        }
        return queryConsUtil.getQueryFromTemplate(MSS_ERR_SUC_TOTAL_EVNT_ANALYSIS_VOICE_TEMPLATE,
                evntAnalysisParameters);
    }

    /**
     * This method is used to get the dropped table query part to be used in the error query
     * @return string representing dropped table query
     */
    private String getDroppedTable(final boolean isConstantEventID) {
        evntAnalysisParameters.put(TABLES_VAR, techPackTables.getErrTables(KEY_TYPE_DROP_CALL));
        evntAnalysisParameters.put(EVENT_RESULT_PARAM, EVENT_RESULT_DROPPED);
        if (isConstantEventID) {
            return queryConsUtil.getQueryFromTemplate(MSS_ERR_SUC_TOTAL_CONSTANT_EVENT_ID_EVNT_ANALYSIS_VOICE_TEMPLATE,
                    evntAnalysisParameters);
        }
        return queryConsUtil.getQueryFromTemplate(MSS_ERR_SUC_TOTAL_EVNT_ANALYSIS_VOICE_TEMPLATE,
                evntAnalysisParameters);
    }

    /**
     * This method is used to get the success table query part to be used in the final query
     * @return string representing success table query
     */
    private String getSuccessTable(final boolean isConstantEventID) {
        evntAnalysisParameters.put(TABLES_VAR, techPackTables.getSucTables());
        evntAnalysisParameters.put(EVENT_RESULT_PARAM, EVENT_RESULT_SUCCESS);
        if (isConstantEventID) {
            return queryConsUtil.getQueryFromTemplate(MSS_ERR_SUC_TOTAL_CONSTANT_EVENT_ID_EVNT_ANALYSIS_VOICE_TEMPLATE,
                    evntAnalysisParameters);
        }
        return queryConsUtil.getQueryFromTemplate(MSS_ERR_SUC_TOTAL_EVNT_ANALYSIS_VOICE_TEMPLATE,
                evntAnalysisParameters);
    }

    /**
     * This method is used to get the location service success table query part to be 
     * used in the final query
     * @return string representing success table query
     */
    private String getLocServiceSuccessTable() {
        evntAnalysisParameters.put(TABLES_VAR, techPackTables.getSucTables(KEY_TYPE_LOC_SERVICE_SUC));
        evntAnalysisParameters.put(EVENT_RESULT_PARAM, EVENT_RESULT_SUCCESS);
        return queryConsUtil.getQueryFromTemplate(MSS_ERR_SUC_TOTAL_EVNT_ANALYSIS_VOICE_TEMPLATE,
                evntAnalysisParameters);
    }

    /**
     * This method is used to get the SMS success table query part to be 
     * used in the final query
     * @return string representing success table query
     */
    private String getSmsSuccessTable() {
        evntAnalysisParameters.put(TABLES_VAR, techPackTables.getSucTables(KEY_TYPE_SMS_SUC));
        evntAnalysisParameters.put(EVENT_RESULT_PARAM, EVENT_RESULT_SUCCESS);
        return queryConsUtil.getQueryFromTemplate(MSS_ERR_SUC_TOTAL_EVNT_ANALYSIS_VOICE_TEMPLATE,
                evntAnalysisParameters);
    }

    /**
     * This method will return a union all clause string
     * @return string representing union all clause
     */
    private String getUnionAll() {
        final StringBuilder unionStr = new StringBuilder();
        unionStr.append("\n");
        unionStr.append("union all");
        unionStr.append("\n");
        return unionStr.toString();
    }

    /**
     * This method will return the type to group type mapping string.
     * @return to group type string used to fetch the group info for the given node type
     */
    private String getGrpRelatedType() {
        if (type.equals(TYPE_MSC)) {
            return GROUP_TYPE_EVENT_SRC_CS;
        } else if (type.equals(TYPE_BSC)) {
            return GROUP_TYPE_HIER3;
        } else if (type.equals(TYPE_CELL)) {
            return GROUP_TYPE_HIER1;
        } else {
            return type;
        }
    }

    /**
     * This method returns the join columns to used in query template     * 
     */
    private List<String> getColumnsJoinColumns() {
        final List<String> tableColumns = new ArrayList<String>();

        if (type.equals(TYPE_TAC)) {
            tableColumns.add(TAC_PARAM.toUpperCase());
        } else if (type.equals(TYPE_MAN)) {
            tableColumns.add(MAN_PARAM.toUpperCase());
        } else if (type.equals(TYPE_MSC)) {
            tableColumns.add(EVENT_SOURCE_SQL_ID);
        } else if (type.equals(TYPE_BSC)) {
            tableColumns.add(CONTROLLER_SQL_ID);
        } else if (type.equals(TYPE_CELL)) {
            tableColumns.add(CELL_SQL_ID.toUpperCase());
        } else if (type.equals(TYPE_IMSI)) {
            tableColumns.add(TYPE_IMSI);
        } else if (type.equals(TYPE_MSISDN)) {
            tableColumns.add(TYPE_MSISDN);
        }
        return tableColumns;
    }

    /**
     * This method is used for junit
     * @param queryConstructorUtils
     */
    public void setQueryConstructorUtils(final QueryConstructorUtils queryConstructorUtils) {
        this.queryConsUtil = queryConstructorUtils;
    }

    /**
     * Used for JUNIT
     * @param queryConsUtil
     */
    public void setQueryConsUtil(final QueryConstructorUtils queryConsUtil) {
        this.queryConsUtil = queryConsUtil;
    }

    /**
     * Enum that represents the raw columns to be selected
     * @author echchik
     *
     */
    private enum RawColumnsToSelect {
        COLUMNS_ORDER_DESPCRITION(new String[] { "RAT", "EVENT_ID", "EVNTSRC_ID", "HIER3_ID", "HIER321_ID", "DATE_ID",
                "HOUR_ID" }),

        COLUMNS_ORDER_1(new String[] { "EVENT_TIME", "MSISDN", "IMSI", "TAC" }),
        //---------
        COLUMNS_ORDER_1_1(new String[] { "EVENT_TIME" }),

        COLUMNS_ORDER_1_2(new String[] { "EXTERNAL_PROTOCOL_ID", "INCOMING_ROUTE", "OUTGOING_ROUTE" }),

        COLUMNS_ORDER_1_3(new String[] { "TAC", "MSISDN", "IMSI" }),
        //---------
        COLUMNS_ORDER_2(new String[] { "EventResult" }),

        COLUMNS_ORDER_3_1(new String[] { "EXTERNAL_CAUSE_CODE" }),

        COLUMNS_ORDER_3_2(new String[] { "INTERNAL_CAUSE_CODE" }),

        COLUMNS_ORDER_3_3(new String[] { "FAULT_CODE" }),

        COLUMNS_ORDER_3_4(new String[] { "INTERNAL_LOCATION_CODE", "BEARER_SERVICE_CODE", "TELE_SERVICE_CODE" }),

        COLUMNS_ORDER_4(new String[] { "CALL_ID_NUM", "TYPE_OF_CALLING_SUB", "CALLING_PARTY_NUM", "CALLED_PARTY_NUM",
                "CALLING_SUB_IMSI", "CALLED_SUB_IMSI", "CALLING_SUB_IMEI", "CALLED_SUB_IMEI", "MS_ROAMING_NUM",
                "DISCONNECT_PARTY", "CALL_DURATION", "SEIZURE_TIME", "ORIGINAL_CALLED_NUM", "REDIRECT_NUM",
                "REDIRECT_COUNTER", "REDIRECT_IMSI", "REDIRECT_SPN", "CALL_POSITION", "EOS_INFO", "RECORD_SEQ_NUM",
                "NETWORK_CALL_REFERENCE", "MCC", "MNC", "RAC", "LAC" }),

        COLUMNS_ORDER_SUBSCRIBER_DESPCRITION(new String[] { "RAT", "EVENT_ID", "EVNTSRC_ID", "HIER3_ID", "HIER321_ID" }),

        COLUMNS_ORDER_SUBSCRIBER_DETAILS(new String[] { "IMSI", "MSISDN", "DATETIME_ID", "TAC", "IMSI_MCC", "IMSI_MNC",
                "ROAMING" }),

        LOC_SERVICE_COLUMNS_ORDER_DESPCRITION(new String[] { "RAT", "EVENT_ID", "EVNTSRC_ID", "HIER3_ID", "HIER321_ID",
                "DATE_ID", "HOUR_ID" }),

        LOC_SERVICE_COLUMNS_ORDER_3(new String[] { "UNSUC_POSITION_REASON", "LCS_CLIENT_TYPE", "TYPE_LOCATION_REQ" }),

        LOC_SERVICE_COLUMNS_ORDER_4(new String[] { "CALL_ID_NUM", "TARGET_MSISDN", "TARGET_IMSI", "TARGET_IMEI",
                "LCS_CLIENT_ID", "POSITION_DELIVERY", "RECORD_SEQ_NUM", "NETWORK_CALL_REFERENCE", "MCC", "MNC", "RAC",
                "LAC" }),

        SMS_COLUMNS_ORDER_DESPCRITION(new String[] { "RAT", "EVENT_ID", "EVNTSRC_ID", "HIER3_ID", "HIER321_ID",
                "DATE_ID", "HOUR_ID" }),

        SMS_COLUMNS_ORDER_3(new String[] { "SMS_RESULT", "MSG_TYPE_INDICATOR", "BEARER_SERVICE_CODE",
                "TELE_SERVICE_CODE" }),

        SMS_COLUMNS_ORDER_4(new String[] { "CALL_ID_NUM", "TYPE_OF_CALLING_SUB", "CALLING_PARTY_NUM",
                "CALLED_PARTY_NUM", "CALLING_SUB_IMSI", "CALLED_SUB_IMSI", "CALLING_SUB_IMEI", "CALLED_SUB_IMEI",
                "CALLING_SUB_IMEISV", "CALLED_SUB_IMEISV", "ORIGINATING_NUM", "DEST_NUM", "SERVICE_CENTRE",
                "ORIGINATING_TIME", "DELIVERY_TIME", "RECORD_SEQ_NUM", "MCC", "MNC", "RAC", "LAC" });

        private RawColumnsToSelect(final String[] listOfCol) {
            colList = listOfCol;
        }

        private final String[] colList;

        public List<String> getRawColumns() {
            final List<String> allCol = new ArrayList<String>();
            for (final String column : colList) {
                allCol.add(column);
            }
            return allCol;
        }
    }
}
