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
package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.QueryConstructorConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.GroupHashId;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.queryconstructor.QueryConstructorUtils;

/**
 * This class provide the generic functionality to generate the query for supported schema
 * and node types
 *
 * This class should be sub classed to provide support for voice, sms and location service
 * separately
 *
 * The Location service extends this class to override few method to generate query
 * for location service schema
 * @author echchik
 *
 */

public abstract class MssEventAnalysisSummaryQueryCreator {

    /*
     * Map to hold the parameters used to construct the query
     */
    protected Map<String, Object> evntAnalysisParameters;;

    /*
     * This object provide raw tables for blocked, dropped and success
     */
    protected TechPackTables techPackTables;

    /*
     * This parameter will decide if raw or aggregation table should be used
     */
    protected boolean isAggregation;

    /*
     * This parameter will decide if TAC exclusion functionality should be considered
     */
    protected boolean toExcludeTac;

    /*
     * This parameter will decide if request if for a EXCLUSIVE_TAC group or a TAC part of
     * EXCLUSIVE_TAC group
     */
    protected boolean isExcludedTacOrGroup;

    /*
     * Holds the time range to be appended to agg view
     */
    protected String timeStringToAppend;

    /*
     * This parameter represent the type for which query should be created
     */
    protected String type;

    /*
     * This object holds the data required by the templates to generate the query
     */
    protected MssSummaryTemplateInfoObject tempInfoForType;

    /*
     * This map provides info about the group join keys, table to use
     */
    protected Map<String, GroupHashId> groups;

    /*
     * This utility is used to create query using template utilities
     */
    @EJB
    protected QueryConstructorUtils queryConsUtil;

    /*
     * This provides the MssSummaryTemplateInfoObject required for the type for which
     * query needs to be created
     */
    @EJB
    protected MssSummaryTemplateInfoToTypeMappings tempInfoToTypeMappings;

    protected enum TypesOfView {
        SUCCESS, DROPPED, BLOCKED, SMS_SUCCESS, SMS_ERROR, LOC_SERVICE_SUCCESS, LOC_SERVICE_ERROR;
    }

    @SuppressWarnings("unchecked")
    public String createQuery(final Map<String, Object> templateParameters) {
        evntAnalysisParameters = new HashMap<String, Object>();
        evntAnalysisParameters.putAll(templateParameters);
        type = (String) evntAnalysisParameters.get(TYPE_PARAM);
        isExcludedTacOrGroup = (Boolean) evntAnalysisParameters.get(IS_EXCULDED_TAC_OR_TACGROUP);
        tempInfoForType = tempInfoToTypeMappings.getTemplateInfoObj(type);
        techPackTables = (TechPackTables) evntAnalysisParameters.get(TECH_PACK_TABLES);
        groups = (Map<String, GroupHashId>) evntAnalysisParameters.get(GROUP_DEFINITIONS);
        String timeRange = (String) evntAnalysisParameters.get(TIMERANGE_PARAM);
        timeStringToAppend = returnAggregateViewTypeWithOutOneMinute(timeRange);
        //EJB may be re-used by glassfish, hence set to false before using again
        isAggregation = false;
        toExcludeTac = false;
        //isExcludeTacOrGroup is true means the groupname is EXCLUSIVE_TAC or drill down on a
        // TAC part of EXCLUSIVE_TAC group, hence we need to go to raw tables
        if (isExcludedTacOrGroup) {
            timeRange = EventDataSourceType.RAW.getValue();
        }
        //If type is IMSI,IMSI group or MSISDN then aggregation tables should not be used
        if (!isImsiOrMsisdnType()) {
            isAggregation = toUseAggregationTables(timeRange);
        }
        if (!isExcludedTacOrGroup && !isAggregation) {
            toExcludeTac = true;
        }

        final AllListInfoData allListInfoData = new AllListInfoData();
        if (templateParameters.containsKey(FAIL_TYPE)) {
            allListInfoData.isftype = true;
            allListInfoData.FAIL_TYPE = (String) templateParameters.get(FAIL_TYPE);
        }
        populateCommonEvntAnalysisParameters(allListInfoData);
        if (isGroupSummaryDrillDown()) {
            allListInfoData.isGrpSummaryDrillDown = true;
            return createGrpSummaryDrilldownQuery(allListInfoData);
        } else if (templateParameters.containsKey(GROUP_NAME_KEY)) {
            return createGrpQuery(allListInfoData);
        } else {
            return createNonGrpQuery(allListInfoData);
        }
    }

    /**
     * This method is used check the given type is IMSI or MSISDN
     * @return true if type is IMSI or MSISDN else false
     */
    protected boolean isImsiOrMsisdnType() {
        if (TYPE_IMSI.equals(type) || TYPE_MSISDN.equals(type)) {
            return true;
        }
        return false;
    }

    /**
     * This method is used to create non group query for all node types supported
     * @param allListInfoData list that hold all data needed for creating final and sub queries
     * @return query representing the node type
     */
    protected abstract String createNonGrpQuery(final AllListInfoData allListInfoData);

    /**
     * This method is used to create  group query for all node types supported
     * @param allListInfoData list that hold all data needed for creating final and sub queries
     * @return query representing the node group type
     */
    protected abstract String createGrpQuery(final AllListInfoData allListInfoData);

    /**
     * This method is used to create group summary drilldown for all node types supported
     * @param allListInfoData list that hold all data needed for creating final and sub queries
     * @return query representing the group summary drill down node type
     */
    protected abstract String createGrpSummaryDrilldownQuery(final AllListInfoData allListInfoData);

    /**
     * This method will create the imsi table part that provide the impacted subscribers data
     * @param imsiJoinTableList alias table to be joined
     * @return query with impacted subscriber part appened
     */
    protected abstract String createImsiTable(final List<String> imsiJoinTableList, final boolean isGrpSummaryDrillDown);

    /**
     * This method is used to check if raw tables or the aggregation tables should be used
     * @param timeRange indicated the time range raw, 15 min or day tables
     * @return true if time range specifies the aggregation else false
     */
    protected boolean toUseAggregationTables(final String timeRange) {
        if (timeRange == null || EventDataSourceType.AGGREGATED_1MIN.getValue().equalsIgnoreCase(timeRange)
                || EventDataSourceType.RAW.getValue().equalsIgnoreCase(timeRange)) {
            return false;
        }
        return true;
    }

    /**
     * This method populate the map of evntAnalysisParameters required to create both non group and
     * group query
     */
    protected void populateCommonEvntAnalysisParameters(final AllListInfoData allListInfoData) {
        evntAnalysisParameters.put(USE_TAC_EXCLUSION_BOOLEAN, toExcludeTac);
        evntAnalysisParameters.put(FILTER_COLUMNS_VAR, tempInfoForType.getFilterColumns());
        evntAnalysisParameters.put(EVENT_DESCRIPTION_COLUMN_VAR, tempInfoForType.getEventDespCloumnNames());
        evntAnalysisParameters.put(SUM_ERR_COLUMNS_ALIAS_LIST_VAR, NO_OF_ERRORS_AGG);
        evntAnalysisParameters.put(SUM_SUC_COLUMNS_ALIAS_LIST_VAR, NO_OF_SUCCESSES_AGG);
        evntAnalysisParameters.put(SUM_TOTAL_COLUMNS_ALIAS_LIST_VAR, TOTAL_EVENTS);
        evntAnalysisParameters.put(EVENT_DESCRIPTION_TABLE_VAR, EVENT_TYPE_TABLE_VIEW);
        evntAnalysisParameters.put(ALL_ALIAS_TABLE_LIST_VAR, allListInfoData.listOfTablesInJoin);
        evntAnalysisParameters.put(ERR_COLUMNS_ALIAS_LIST_VAR, allListInfoData.listOfErrColumnAlias);
        evntAnalysisParameters.put(SUC_COLUMNS_ALIAS_LIST_VAR, allListInfoData.listOfSucColumnAlias);
        evntAnalysisParameters.put(TOTAL_COLUMNS_ALIAS_LIST_VAR, allListInfoData.listOfAllColumnAlias);
        final List<String> filterCol = tempInfoForType.getFilterColumns();
        final List<String> imsiFilterCol = new ArrayList<String>();
        imsiFilterCol.addAll(filterCol);
        //if the summary report is for IMSI group then no need to add IMSI column
        //as the filter column will already have IMSI column entry
        if (!TYPE_IMSI.equals(type)) {
            imsiFilterCol.add("IMSI");
        }
        // if TAC or TAC group is not related, then add TAC into imsi filter column
        if (!isExcludedTacOrGroup && toAddTacToRawFilterColumns(imsiFilterCol)) {
            imsiFilterCol.add(TAC_PARAM_UPPER_CASE);
        }
        if (evntAnalysisParameters.containsKey(HOUR_PARAM)) {
            imsiFilterCol.add(HOUR_SQL_PARAM);
        } else if (evntAnalysisParameters.containsKey(DAY_PARAM)) {
            imsiFilterCol.add(DATETIME_ID);
        } else if (evntAnalysisParameters.containsKey(SUBBI_CELL_SQL_ID)) {
            imsiFilterCol.add(CELL_SQL_ID);
        }
        populateImsiSubscriberVar();
        evntAnalysisParameters.put(IMSI_FILTER_COLUMNS_VAR, imsiFilterCol);
    }

    protected void populateImsiSubscriberVar() {
        if (isImsiNonGroup() || TYPE_MSISDN.equals(type)) {
            evntAnalysisParameters.put(IMPACTED_SUBSCRIBER_VAR, "");
        } else {
            evntAnalysisParameters.put(IMPACTED_SUBSCRIBER_VAR, IMPACTED_SUBSCRIBER_SQL_ENTRY);
        }
    }

    protected boolean isImsiNonGroup() {
        return TYPE_IMSI.equals(type) && !evntAnalysisParameters.containsKey(GROUP_NAME_KEY);
    }

    /**
     * This method populate the map of evntAnalysisParameters required to create both non group query
     */
    protected void populateEventAnalysisParametersNonGrp() {
        evntAnalysisParameters.put(COMMON_COLUMNS_VAR, tempInfoForType.getcommonColumns());
        evntAnalysisParameters.put(JOIN_COLUMNS_VAR, tempInfoForType.getJoinColumns());
        evntAnalysisParameters.put(EXTRA_TABLE_JOIN_COLUMNS_VAR, tempInfoForType.getExtraTableJoin());
        evntAnalysisParameters.put(EXTRA_TABLE_VAR, tempInfoForType.getExtraTable());
        evntAnalysisParameters.put(SELECT_COLUMNS_VAR, tempInfoForType.getSelectColumns());
        if (isImsiOrMsisdnType()) {
            evntAnalysisParameters.put(TYPE_TO_NODE_TABLE_VIEW_VAR, "");
        } else {
            evntAnalysisParameters.put(TYPE_TO_NODE_TABLE_VIEW_VAR, TOPOLOGY_TABLE_VIEW);
        }
    }

    /**
     * This method populate the map of evntAnalysisParameters required to create group query
     */
    protected void populateEventAnalysisParametersGrp() {
        final String grpType = getGrpRelatedType();
        final GroupHashId grpDef = groups.get(grpType);
        final String grpNameColumn = grpDef.getGroupNameColumn();
        final List<String> selectJoinColumnsForGrp = new ArrayList<String>();
        selectJoinColumnsForGrp.add(grpNameColumn);
        evntAnalysisParameters.put(JOIN_COLUMNS_VAR, selectJoinColumnsForGrp);
        evntAnalysisParameters.put(EXTRA_TABLE_JOIN_COLUMNS_VAR, grpDef.getGroupKeys());
        evntAnalysisParameters.put(EXTRA_TABLE_VAR, grpDef.getTableName());
        evntAnalysisParameters.put(SELECT_COLUMNS_VAR, selectJoinColumnsForGrp);
        evntAnalysisParameters.put(TYPE_TO_NODE_TABLE_VIEW_VAR, "");
        evntAnalysisParameters.put(COMMON_COLUMNS_VAR, tempInfoForType.getcommonColumns());
    }

    /**
     * This method populate the map of evntAnalysisParameters required to create group summary
     * drill down query
     */
    protected void populateEventAnalysisParametersGrpSummaryDrilldownOnEvntId() {
        final String grpType = getGrpRelatedType();
        final GroupHashId grpDef = groups.get(grpType);
        final String grpNameColumn = grpDef.getGroupNameColumn();
        final List<String> grpNameColList = new ArrayList<String>();
        grpNameColList.add(grpNameColumn);
        final List<String> joinColumns = new ArrayList<String>();
        joinColumns.addAll(tempInfoForType.getJoinColumns());
        joinColumns.addAll(tempInfoForType.getcommonColumns());
        //common column should be used only in the total template
        evntAnalysisParameters.put(COMMON_COLUMNS_VAR, tempInfoForType.getcommonColumns());
        evntAnalysisParameters.put(JOIN_COLUMNS_VAR, joinColumns);
        evntAnalysisParameters.put(EXTRA_TABLE_JOIN_COLUMNS_VAR, grpNameColList);
        evntAnalysisParameters.put(EXTRA_TABLE_VAR, grpDef.getTableName());
        evntAnalysisParameters.put(SELECT_COLUMNS_VAR, tempInfoForType.getSelectColumns());
        evntAnalysisParameters.put(TYPE_TO_NODE_TABLE_VIEW_VAR, TOPOLOGY_TABLE_VIEW);
    }

    /**
     * This method is used to append the topology query part for non group nodes
     * @param queryStr query to which topology table need to be joined
     * @return query with topology part appended
     */
    protected StringBuilder getJoinOnTopology(final StringBuilder queryStr, final AllListInfoData allListInfoData) {
        queryStr.append("\n");
        final String leftJoinTypeToNdeTable = createLeftJoinWithTypeToNodeTable(allListInfoData.listOfTablesInJoin);
        queryStr.append(leftJoinTypeToNdeTable);
        return queryStr;
    }

    /**
     * utility method used to append the full outer join query part for query passes as parameter
     * @param queryStr to which the full outer join clause to append
     * @return query with appened full outer join clause
     */
    protected void appendFullOuterJoin(final StringBuilder queryStr) {
        queryStr.append("\n");
        queryStr.append(FULL_OUTER_JOIN_STRING);
        queryStr.append("\n");
    }

    /**
     * utility method used to append the union all clause for query passes as parameter
     * @param queryStr to which the full outer join clause to append
     * @return query with appened full outer join clause
     */
    protected void appendUnionAll(final StringBuilder queryStr) {
        queryStr.append("\n");
        queryStr.append(UNION_ALL);
        queryStr.append("\n");
    }

    /**
     * This method is used to populated success aggregation sub query specific data
     */
    @SuppressWarnings("unchecked")
    protected void updateFilterColumnsForSuccessAggregation() {
        final List<String> filterCol = (List<String>) evntAnalysisParameters.get(FILTER_COLUMNS_VAR);
        final List<String> aggFilterColumns = new ArrayList<String>();
        aggFilterColumns.addAll(filterCol);
        aggFilterColumns.add(NO_OF_SUCCESSES_AGG);
        evntAnalysisParameters.put(FILTER_COLUMNS_VAR, aggFilterColumns);
    }

    /**
     * This method is used to populated error aggregation view sub query specific data
     */
    @SuppressWarnings("unchecked")
    protected void updateFilterColumnsForErrorAggregation() {
        final List<String> filterCol = (List<String>) evntAnalysisParameters.get(FILTER_COLUMNS_VAR);
        final List<String> aggFilterColumns = new ArrayList<String>();
        aggFilterColumns.addAll(filterCol);
        if (!aggFilterColumns.contains(NO_OF_ERRORS_AGG)) {
            aggFilterColumns.add(NO_OF_ERRORS_AGG);
        }
        evntAnalysisParameters.put(FILTER_COLUMNS_VAR, aggFilterColumns);
    }

    /**
     * This method is used to populate sub query specific data with tac exclusion related
     * data
     */
    @SuppressWarnings("unchecked")
    protected void updateFilterColumnsForRaw() {
        final List<String> filterCol = (List<String>) evntAnalysisParameters.get(FILTER_COLUMNS_VAR);
        if (!toAddTacToRawFilterColumns(filterCol)) {
            return;
        }
        final List<String> rawFilterColumns = new ArrayList<String>();
        rawFilterColumns.addAll(filterCol);
        rawFilterColumns.add(TAC_PARAM_UPPER_CASE);
        evntAnalysisParameters.put(FILTER_COLUMNS_VAR, rawFilterColumns);
    }

    protected boolean toAddTacToRawFilterColumns(final List<String> filterColumns) {
        if (toExcludeTac && !filterColumns.contains(TAC_PARAM_UPPER_CASE)) {
            return true;
        }
        return false;
    }

    /**
     * This method will create the on clause required by the joins for the
     * list of alias table to be joined
     * The first entry in the list is always considered as the join table
     * @param listOfTablesInJoin alias table to be joined
     * @return query with on clause part appended
     */
    protected String getOnClause(final List<String> listOfTablesInJoin, final boolean isGrpSummaryDrillDown) {
        final String joinTable = listOfTablesInJoin.get(0);
        final List<String> aliasTables = listOfTablesInJoin.subList(1, listOfTablesInJoin.size());

        evntAnalysisParameters.put(ON_CLAUSE_JOIN_TABLE, joinTable);
        evntAnalysisParameters.put(ON_CLAUSE_LEFT_OUTER_JOIN_ALIAS_TABLE, aliasTables);
        final String template = getTemplateToUseForOnClause(isGrpSummaryDrillDown);
        return queryConsUtil.getQueryFromTemplate(template, evntAnalysisParameters);
    }

    /**
     * This method will provide the join query part to join the event description table
     * @param listOfTablesInJoin alias table to be joined
     * @return query part to join the event description table
     */
    protected String createLeftJoinWithEvntDespTable(final List<String> listOfTablesInJoin) {
        evntAnalysisParameters.put(COLUMNS, tempInfoForType.getcommonColumns());
        evntAnalysisParameters.put(TEMP_JOIN_TABLES, tempInfoForType.getEventDespTable());
        evntAnalysisParameters.put(TEMP_JOIN_VIEW_VAR, EVENT_TYPE_TABLE_VIEW);
        evntAnalysisParameters.put(ON_CLAUSE_LEFT_OUTER_JOIN_ALIAS_TABLE, listOfTablesInJoin);
        return queryConsUtil.getQueryFromTemplate(MSS_LEFT_OUTER_JOIN_TEMPLATE, evntAnalysisParameters);
    }

    /**
     * This method will provide the join query part to join the topology description table
     * @param listOfTablesInJoin alias table to be joined
     * @return query part to join the topology description table
     */
    protected String createLeftJoinWithTypeToNodeTable(final List<String> listOfTablesInJoin) {
        evntAnalysisParameters.put(COLUMNS, tempInfoForType.getJoinColumns());
        evntAnalysisParameters.put(TEMP_JOIN_TABLES, tempInfoForType.getTypeToNodeTable());
        evntAnalysisParameters.put(TEMP_JOIN_VIEW_VAR, TOPOLOGY_TABLE_VIEW);
        evntAnalysisParameters.put(TOPOLOGY_FILTER_COLUMNS, tempInfoForType.getSelectColumns());
        evntAnalysisParameters.put(ON_CLAUSE_LEFT_OUTER_JOIN_ALIAS_TABLE, listOfTablesInJoin);
        return queryConsUtil.getQueryFromTemplate(MSS_LEFT_OUTER_JOIN_TEMPLATE, evntAnalysisParameters);
    }

    /**
     * This method will return the type to group type mapping string.
     * @return to group type string used to fetch the group info for the given node type
     */
    protected String getGrpRelatedType() {
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
     * This method will return the template to use for success sub query
     * @param isGrpSummaryDrillDown true if drill down from a node group type else false
     * @return the template to use
     */
    protected String getTemplateToUseForErrNSucTables(final boolean isGrpSummaryDrillDown) {
        if (isGrpSummaryDrillDown) {
            return MSS_SUMMARY_DRILLDOWN_EVNTID_EVNT_ANALYSIS_VOICE_TEMPLATE;
        } else if (isSubBISummary()) {
            return MSS_SUMMARY_SUBBI_EVNT_ANALYSIS_VOICE_TEMPLATE;
        }
        return MSS_SUMMARY_EVNT_ANALYSIS_VOICE_TEMPLATE;
    }

    /**
     * This method will return the template to use for error sub query
     * @param isGrpSummaryDrillDown true if drill down from a node group type else false
     * @return the template to use
     */
    protected String getTemplateToUseForCombinedErrView(final boolean isGrpSummaryDrillDown) {
        if (isGrpSummaryDrillDown) {
            return MSS_SUMMARY_DRILLDOWN_EVNTID_EVNT_ANALYSIS_VOICE_ERRORVIEW_TEMPLATE;
        } else if (isSubBISummary()) {
            return MSS_SUMMARY_SUBBI_EVNT_ANALYSIS_VOICE_ERRORVIEW_TEMPLATE;
        }
        return MSS_SUMMARY_EVNT_ANALYSIS_VOICE_ERRORVIEW_TEMPLATE;
    }

    /**
     * This method will return the template to use for impacted subscribers sub query
     * @param isGrpSummaryDrillDown true if drill down from a node group type else false
     * @return the template to use
     */
    protected String getTemplateToUseForImsi(final boolean isGrpSummaryDrillDown) {
        if (isGrpSummaryDrillDown) {
            return MSS_SUMMARY_DRILLDOWN_EVNTID_EVNT_ANALYSIS_VOICE_IMSI_TEMPLATE;
        } else if (isSubBISummary()) {
            return MSS_SUMMARY_SUBBI_EVNT_ANALYSIS_VOICE_IMSI_TEMPLATE;
        }
        return MSS_SUMMARY_EVNT_ANALYSIS_VOICE_IMSI_TEMPLATE;
    }

    /**
     * This method will return the template to use for impacted subscribers total sub query
     * @param isGrpSummaryDrillDown true if drill down from a node group type else false
     * @return the template to use
     */
    protected String getTemplateToUseForImsiTotal(final boolean isGrpSummaryDrillDown) {
        if (isGrpSummaryDrillDown) {
            return MSS_SUMMARY_DRILLDOWN_EVNTID_EVNT_ANALYSIS_VOICE_IMSI_TOTAL_TEMPLATE;
        }
        return MSS_SUMMARY_EVNT_ANALYSIS_VOICE_IMSI_TOTAL_TEMPLATE;
    }

    /**
     * This method will return the template to use for on clause sub query
     * @param isGrpSummaryDrillDown true if drill down from a node group type else false
     * @return the template to use
     */
    protected String getTemplateToUseForOnClause(final boolean isGrpSummaryDrillDown) {
        if (isGrpSummaryDrillDown) {
            return MSS_ON_CLAUSE_WITH_SINGLE_MULTI_JOIN_TEMPLATE;
        }
        return MSS_ON_CLAUSE_TEMPLATE;
    }

    /**
     * This method is used to check whether the group summary drill down query needs to be
     * created
     * @return true if group summary drill down query needs to be created else false
     */
    protected boolean isGroupSummaryDrillDown() {
        final String evntID = (String) evntAnalysisParameters.get(EVENT_ID_PARAM);
        return (evntAnalysisParameters.containsKey(GROUP_NAME_KEY) && StringUtils.isNotBlank(evntID));

    }

    /**
     * This method will return the list of aggregation table to use for given Aggregation view
     * @param typeOfView represents SUCCESS,BLOCKED, DROPPED aggregation view
     * @return list of aggregation table to use
     */
    protected List<String> getAggTableList(final TypesOfView typeOfView) {
        final List<String> aggTableView;
        if (TypesOfView.BLOCKED.equals(typeOfView)) {
            aggTableView = tempInfoForType.getBlockedAggTables();
        } else if (TypesOfView.DROPPED.equals(typeOfView)) {
            aggTableView = tempInfoForType.getDroppedAggTables();
        } else if (TypesOfView.LOC_SERVICE_ERROR.equals(typeOfView)) {
            aggTableView = tempInfoForType.getLocServiceErrAggTables();
        } else if (TypesOfView.LOC_SERVICE_SUCCESS.equals(typeOfView)) {
            aggTableView = tempInfoForType.getLocServiceSucAggTables();
        } else if (TypesOfView.SMS_ERROR.equals(typeOfView)) {
            aggTableView = tempInfoForType.getSmsErrAggTables();
        } else if (TypesOfView.SMS_SUCCESS.equals(typeOfView)) {
            aggTableView = tempInfoForType.getSmsSucAggTables();
        } else {
            aggTableView = tempInfoForType.getSucAggTables();
        }
        return updateAggTableWithTimeRange(aggTableView);
    }

    /**
     * This method will return the list of raw table to use for given raw view
     * @param typeOfView represents SUCCESS,BLOCKED, DROPPED error view
     * @return list of aggregation table to use
     */
    protected List<String> getRawTableList(final TypesOfView typeOfView) {
        if (TypesOfView.BLOCKED.equals(typeOfView)) {
            return techPackTables.getErrTables(KEY_TYPE_ERR);
        } else if (TypesOfView.DROPPED.equals(typeOfView)) {
            return techPackTables.getErrTables(KEY_TYPE_DROP_CALL);
        } else if (TypesOfView.LOC_SERVICE_ERROR.equals(typeOfView)) {
            return techPackTables.getErrTables(KEY_TYPE_LOC_SERVICE_ERR);
        } else if (TypesOfView.LOC_SERVICE_SUCCESS.equals(typeOfView)) {
            return techPackTables.getSucTables(KEY_TYPE_LOC_SERVICE_SUC);
        } else if (TypesOfView.SMS_ERROR.equals(typeOfView)) {
            return techPackTables.getErrTables(KEY_TYPE_SMS_ERR);
        } else if (TypesOfView.SMS_SUCCESS.equals(typeOfView)) {
            return techPackTables.getSucTables(KEY_TYPE_SMS_SUC);
        } else {
            return techPackTables.getSucTables();
        }
    }

    /**
     * This method is used to append the 15MIN or DAY aggregation time range string
     * to the list of aggregation table to use
     * @param listOfAggTableView list of aggregation tables
     * @return list of aggregation tables appened with time range string(15MIN or DAY)
     */
    protected List<String> updateAggTableWithTimeRange(final List<String> listOfAggTableView) {
        final List<String> aggViewListWithTimeString = new ArrayList<String>();
        for (final String tableView : listOfAggTableView) {
            final StringBuilder tableViewWithTimeString = new StringBuilder(tableView);
            tableViewWithTimeString.append(timeStringToAppend);
            aggViewListWithTimeString.add(tableViewWithTimeString.toString());
        }
        return aggViewListWithTimeString;
    }

    /**
     * This method is used to check whether query to generate is for SUBBI summary
     * @return true if the query parameters has SUBBI related parameters else false
     */
    protected boolean isSubBISummary() {
        if (evntAnalysisParameters.containsKey(HOUR_PARAM) || evntAnalysisParameters.containsKey(DAY_PARAM)
                || evntAnalysisParameters.containsKey(SUBBI_CELL_SQL_ID)) {
            return true;
        }
        return false;
    }

    /**
     * This method is used to update the template parameters in case the query generated is for SUBBI
     */
    @SuppressWarnings("unchecked")
    protected void updateEventAnalysisParamsForSubBI() {
        if (TYPE_IMSI.equals(type) || TYPE_MSISDN.equals(type)) {
            final List<String> selectColumns = (List<String>) evntAnalysisParameters.get(SELECT_COLUMNS_VAR);
            final List<String> updatedSelectColumns = new ArrayList<String>();
            updatedSelectColumns.addAll(selectColumns);
            evntAnalysisParameters.put(SELECT_COLUMNS_VAR, updatedSelectColumns);
            final List<String> filterColumns = (List<String>) evntAnalysisParameters.get(FILTER_COLUMNS_VAR);
            final List<String> updatedFilterColumns = new ArrayList<String>();
            updatedFilterColumns.addAll(filterColumns);
            final String hourId = (String) evntAnalysisParameters.get(HOUR_PARAM);
            if (hourId != null) {
                updatedSelectColumns.add(HOUR_PARAM);
                updatedFilterColumns.add(HOUR_SQL_PARAM);
                evntAnalysisParameters.put(CONSTANT_COLUMN_NAME, HOUR_PARAM);
                evntAnalysisParameters.put(CONSTANT_COLUMN_VALUE, getStringWithSingleQuotes(hourId));
            }
            final String day = (String) evntAnalysisParameters.get(DAY_PARAM);
            if (day != null) {
                updatedSelectColumns.add(DAY_PARAM);
                updatedFilterColumns.add(DATETIME_ID);
                evntAnalysisParameters.put(CONSTANT_COLUMN_NAME, DAY_PARAM);
                evntAnalysisParameters.put(CONSTANT_COLUMN_VALUE, getStringWithSingleQuotes(day));
            }
            if (evntAnalysisParameters.containsKey(SUBBI_CELL_SQL_ID)) {
                final Object cellId = evntAnalysisParameters.get(SUBBI_CELL_SQL_ID);
                updatedSelectColumns.add(CELL_SQL_ID);
                updatedFilterColumns.add(CELL_SQL_ID);
                evntAnalysisParameters.put(CONSTANT_COLUMN_NAME, CELL_SQL_ID);
                evntAnalysisParameters.put(CONSTANT_COLUMN_VALUE, cellId);
            }
            evntAnalysisParameters.put(FILTER_COLUMNS_VAR, updatedFilterColumns);
        }
    }

    protected String getStringWithSingleQuotes(final String string) {
        return "\'" + string + "\'";
    }

    /**
     * Used for JUNIT
     * @param queryConstructorUtils
     */
    public void setQueryConstructorUtils(final QueryConstructorUtils queryConstructorUtils) {
        this.queryConsUtil = queryConstructorUtils;
    }

    /**
     * Used for JUNIT
     * @param sumTempInfoToTypeMappings
     */
    public void setSumTempInfoToTypeMappings(final MssSummaryTemplateInfoToTypeMappings sumTempInfoToTypeMappings) {
        this.tempInfoToTypeMappings = sumTempInfoToTypeMappings;
    }

    /**
     * Used for JUNIT
     * @param tempInfoToTypeMappings
     */
    public void setTempInfoToTypeMappings(final MssSummaryTemplateInfoToTypeMappings tempInfoToTypeMappings) {
        this.tempInfoToTypeMappings = tempInfoToTypeMappings;
    }

    /**
     * This class is used hold query specific data used for different query types like summary,
     * group summary and group summary drilldown for all supported node types
     * @author echchik
     *
     */
    public class AllListInfoData {
        final List<String> listOfTablesInJoin = new ArrayList<String>();

        final List<String> listOfErrColumnAlias = new ArrayList<String>();

        final List<String> listOfSucColumnAlias = new ArrayList<String>();

        final List<String> listOfAllColumnAlias = new ArrayList<String>();

        boolean isGrpSummaryDrillDown;

        boolean isftype;

        String FAIL_TYPE;
    }
}
