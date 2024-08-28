/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.queryconstructor.impl.mss.subbi.summary;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.QueryConstructorConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;

import com.ericsson.eniq.events.server.common.GroupHashId;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.queryconstructor.QueryConstructorUtils;

public abstract class MssSubscriberBISummaryQueryCreator {

    /*
     * Map to hold the parameters used to construct the query
     */
    protected Map<String, Object> subBIParameters;;

    /*
     * This object provide raw tables for blocked, dropped and success
     */
    protected TechPackTables techPackTables;

    /*
     * This map provides info about the group join keys, table to use
     */
    protected Map<String, GroupHashId> groups;

    protected String type;

    protected static final List<String> FAILURE_COMMON_COLUMN;

    protected static final List<String> BUSY_HOUR_COMMON_COLUMN;

    protected static final List<String> BUSY_DAY_COMMON_COLUMN;

    protected static final List<String> CELL_ANALYSIS_COMMON_COLUMN;

    protected static final List<String> TERMINAL_ANALYSIS_COMMON_COLUMN;

    /* 
     * This utility is used to create query using template utilities
     */
    @EJB
    protected QueryConstructorUtils queryConsUtil;

    static {
        FAILURE_COMMON_COLUMN = new ArrayList<String>(1);
        BUSY_HOUR_COMMON_COLUMN = new ArrayList<String>(1);
        BUSY_DAY_COMMON_COLUMN = new ArrayList<String>(1);
        CELL_ANALYSIS_COMMON_COLUMN = new ArrayList<String>(1);
        TERMINAL_ANALYSIS_COMMON_COLUMN = new ArrayList<String>(1);

        FAILURE_COMMON_COLUMN.add(EVENT_ID_SQL_PARAM);
        BUSY_HOUR_COMMON_COLUMN.add(HOUR_SQL_PARAM);
        BUSY_DAY_COMMON_COLUMN.add(DATETIME_ID);
        CELL_ANALYSIS_COMMON_COLUMN.add(CELL_SQL_ID);

        TERMINAL_ANALYSIS_COMMON_COLUMN.add(TAC_PARAM_UPPER_CASE);
        TERMINAL_ANALYSIS_COMMON_COLUMN.add("IMEISV");
        TERMINAL_ANALYSIS_COMMON_COLUMN.add(EVENT_ID);
    }

    /**
     * This method is used to create query for failure summary of subscriber 
     * business intelligence
     * @param templateParameters parameters required to generate failure subbi query
     * @return query used to fetch failure subbi data
     */
    @SuppressWarnings("unchecked")
    public List<String> createFailureQuery(final Map<String, Object> templateParameters) {
        final AllListInfoData allListInfoData = new AllListInfoData();
        populateCommonParameters(templateParameters, allListInfoData);
        subBIParameters.put(COMMON_COLUMNS_VAR, FAILURE_COMMON_COLUMN);
        final List<String> filterColumns = new ArrayList<String>();
        filterColumns.addAll(FAILURE_COMMON_COLUMN);
        filterColumns.addAll(getJoinColumn());
        subBIParameters.put(FILTER_COLUMNS_VAR, filterColumns);
        final StringBuilder queryStr = createQuery(allListInfoData);
        final String leftJoinEvntTable = createLeftJoinWithEvntDespTable(allListInfoData.listOfTablesInJoin);
        queryStr.append(leftJoinEvntTable);
        subBIParameters.put(ALL_TABLES_QUERY_PART, queryStr.toString());
        final String totalFailureQuery = queryConsUtil.getQueryFromTemplate(MSS_SUBBI_FAILURES_VOICE_TEMPLATE,
                subBIParameters);
        final List<String> allQueries = new ArrayList<String>();
        allQueries.add(totalFailureQuery);
        return allQueries;
    }

    /**
     * This method is used to create query for busy hour summary of subscriber 
     * business intelligence
     * @param templateParameters parameters required to generate busy hour subbi query
     * @return query used to fetch busy hour subbi data
     */
    @SuppressWarnings("unchecked")
    public List<String> createBusyHourQuery(final Map<String, Object> templateParameters) {
        final AllListInfoData allListInfoData = new AllListInfoData();
        populateCommonParameters(templateParameters, allListInfoData);
        subBIParameters.put(COMMON_COLUMNS_VAR, BUSY_HOUR_COMMON_COLUMN);
        final List<String> filterColumns = new ArrayList<String>();
        filterColumns.addAll(BUSY_HOUR_COMMON_COLUMN);
        filterColumns.addAll(getJoinColumn());
        subBIParameters.put(FILTER_COLUMNS_VAR, filterColumns);
        final StringBuilder queryStr = createQuery(allListInfoData);
        subBIParameters.put(ALL_TABLES_QUERY_PART, queryStr.toString());
        final String totalBusyHourQuery = queryConsUtil.getQueryFromTemplate(MSS_SUBBI_BUSY_HOUR_VOICE_TEMPLATE,
                subBIParameters);
        final List<String> allQueries = new ArrayList<String>();
        allQueries.add(totalBusyHourQuery);
        return allQueries;
    }

    /**
     * This method is used to create query for busy day summary of subscriber 
     * business intelligence
     * @param templateParameters parameters required to generate busy day subbi query
     * @return query used to fetch busy day subbi data
     */
    @SuppressWarnings("unchecked")
    public List<String> createBusyDayQuery(final Map<String, Object> templateParameters) {
        final AllListInfoData allListInfoData = new AllListInfoData();
        populateCommonParameters(templateParameters, allListInfoData);
        subBIParameters.put(COMMON_COLUMNS_VAR, BUSY_DAY_COMMON_COLUMN);
        final List<String> filterColumns = new ArrayList<String>();
        filterColumns.addAll(BUSY_DAY_COMMON_COLUMN);
        filterColumns.addAll(getJoinColumn());
        subBIParameters.put(FILTER_COLUMNS_VAR, filterColumns);
        final StringBuilder queryStr = createQuery(allListInfoData);
        subBIParameters.put(ALL_TABLES_QUERY_PART, queryStr.toString());
        final String totalBusyDayQuery = queryConsUtil.getQueryFromTemplate(MSS_SUBBI_BUSY_DAY_VOICE_TEMPLATE,
                subBIParameters);
        final List<String> allQueries = new ArrayList<String>();
        allQueries.add(totalBusyDayQuery);
        return allQueries;
    }

    /**
     * This method is used to create query for cell analysis summary of subscriber 
     * business intelligence
     * @param templateParameters parameters required to generate busy day subbi query
     * @return query used to fetch busy day cell analysis data
     */
    @SuppressWarnings("unchecked")
    public List<String> createCellAnalysisQuery(final Map<String, Object> templateParameters) {
        final AllListInfoData allListInfoData = new AllListInfoData();
        populateCommonParameters(templateParameters, allListInfoData);
        subBIParameters.put(COMMON_COLUMNS_VAR, CELL_ANALYSIS_COMMON_COLUMN);
        final List<String> filterColumns = new ArrayList<String>();
        filterColumns.addAll(CELL_ANALYSIS_COMMON_COLUMN);
        filterColumns.addAll(getJoinColumn());
        subBIParameters.put(FILTER_COLUMNS_VAR, filterColumns);
        final StringBuilder queryStr = createQuery(allListInfoData);
        subBIParameters.put(ALL_TABLES_QUERY_PART, queryStr.toString());
        final String totalCellAnalysisQuery = queryConsUtil.getQueryFromTemplate(MSS_SUBBI_CELL_ANALYSIS_TEMPLATE,
                subBIParameters);
        final List<String> allQueries = new ArrayList<String>();
        allQueries.add(totalCellAnalysisQuery);
        return allQueries;
    }

    /**
     * This method is used to create query for Terminal analysis summary of subscriber 
     * business intelligence
     * @param templateParameters parameters required to generate busy day subbi query
     * @return query used to fetch busy day Terminal analysis data
     */
    @SuppressWarnings("unchecked")
    public List<String> createTerminalAnalysisQuery(final Map<String, Object> templateParameters) {
        final AllListInfoData allListInfoData = new AllListInfoData();
        populateCommonParameters(templateParameters, allListInfoData);
        subBIParameters.put(TYPE_PARAM, type);
        final List<String> commonColumns = new ArrayList<String>();
        commonColumns.addAll(TERMINAL_ANALYSIS_COMMON_COLUMN);
        commonColumns.addAll(getJoinColumn());
        subBIParameters.put(COMMON_COLUMNS_VAR, commonColumns);
        final List<String> filterColumns = new ArrayList<String>();
        filterColumns.addAll(TERMINAL_ANALYSIS_COMMON_COLUMN);
        filterColumns.addAll(getJoinColumn());
        filterColumns.add(DATETIME_ID);
        subBIParameters.put(FILTER_COLUMNS_VAR, filterColumns);
        final StringBuilder queryStr = createTerminalAnalysisQuery(allListInfoData);
        subBIParameters.put(ALL_TABLES_QUERY_PART, queryStr.toString());
        final String totalCellAnalysisQuery = queryConsUtil.getQueryFromTemplate(MSS_SUBBI_TERMINAL_ANALYSIS_TEMPLATE,
                subBIParameters);
        final List<String> allQueries = new ArrayList<String>();
        allQueries.add(totalCellAnalysisQuery);
        return allQueries;
    }

    /**
     * This method returns query joining all tables required by both non group and group nodes
     * @return query part representing the joined tables
     */
    protected abstract StringBuilder createQuery(final AllListInfoData allListInfoData);

    /**
     * This method returns query joining all tables required by both non group and group
     * terminal analysis
     * @return query part representing the joined tables
     */
    protected abstract StringBuilder createTerminalAnalysisQuery(final AllListInfoData allListInfoData);

    /**
     * Populates subBIparameters map that used to create subbi query
     * @param templateParameters parameters required to generate busy day subbi query
     * @param allListInfoData provides info related to sub queries
     */
    @SuppressWarnings("unchecked")
    protected void populateCommonParameters(final Map<String, Object> templateParameters,
            final AllListInfoData allListInfoData) {
        subBIParameters = new HashMap<String, Object>();
        subBIParameters.putAll(templateParameters);
        techPackTables = (TechPackTables) subBIParameters.get(TECH_PACK_TABLES);
        groups = (Map<String, GroupHashId>) subBIParameters.get(GROUP_DEFINITIONS);
        type = (String) subBIParameters.get(TYPE_PARAM);
        subBIParameters.put(JOIN_COLUMNS_VAR, getJoinColumn());
        subBIParameters.put(SUM_ERR_COLUMNS_ALIAS_LIST_VAR, NO_OF_ERRORS_AGG);
        subBIParameters.put(SUM_SUC_COLUMNS_ALIAS_LIST_VAR, NO_OF_SUCCESSES_AGG);
        subBIParameters.put(SUM_TOTAL_COLUMNS_ALIAS_LIST_VAR, TOTAL_EVENTS);
        subBIParameters.put(EVENT_DESCRIPTION_TABLE_VAR, EVENT_TYPE_TABLE_VIEW);
        subBIParameters.put(ERR_COLUMNS_ALIAS_LIST_VAR, allListInfoData.listOfErrColumnAlias);
        subBIParameters.put(SUC_COLUMNS_ALIAS_LIST_VAR, allListInfoData.listOfSucColumnAlias);
        subBIParameters.put(ALL_ALIAS_TABLE_LIST_VAR, allListInfoData.listOfTablesInJoin);
        //tac exclusion should be true as SUBBI is for IMSI and MSISDN and will always use raw tables
        subBIParameters.put(USE_TAC_EXCLUSION_BOOLEAN, true);
        if (templateParameters.containsKey(GROUP_NAME_KEY)) {
            populateParametersForImsiGroup();
        }
    }

    /**
     * Populates subBIparameters map with group related data that used to create subbi query
     */
    protected void populateParametersForImsiGroup() {
        final GroupHashId grpDef = groups.get(IMSI_PARAM_UPPER_CASE);
        final String grpNameColumn = grpDef.getGroupNameColumn();
        final List<String> selectJoinColumnsForGrp = new ArrayList<String>();
        selectJoinColumnsForGrp.add(grpNameColumn);
        subBIParameters.put(JOIN_COLUMNS_VAR, selectJoinColumnsForGrp);
        subBIParameters.put(EXTRA_TABLE_JOIN_COLUMNS_VAR, grpDef.getGroupKeys());
        subBIParameters.put(EXTRA_TABLE_VAR, grpDef.getTableName());
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
     * This method will create the on clause required by the joins for the 
     * list of alias table to be joined
     * The first entry in the list is always considered as the join table
     * @param listOfTablesInJoin alias table to be joined
     * @return query with on clause part appended
     */
    protected String getOnClause(final List<String> listOfTablesInJoin) {
        final String joinTable = listOfTablesInJoin.get(0);
        final List<String> aliasTables = listOfTablesInJoin.subList(1, listOfTablesInJoin.size());
        subBIParameters.put(ON_CLAUSE_JOIN_TABLE, joinTable);
        subBIParameters.put(ON_CLAUSE_LEFT_OUTER_JOIN_ALIAS_TABLE, aliasTables);
        final String template = MSS_ON_CLAUSE_WITH_MULTI_JOIN_COMMON_COLUMNS_TEMPLATE;
        return queryConsUtil.getQueryFromTemplate(template, subBIParameters);
    }

    /**
     * This method will provide the join query part to join the event description table
     * @param listOfTablesInJoin alias table to be joined
     * @return query part to join the event description table
     */
    protected String createLeftJoinWithEvntDespTable(final List<String> listOfTablesInJoin) {
        final List<String> evntDespTable = new ArrayList<String>();
        evntDespTable.add(DIM_E_MSS_EVNTTYPE);
        final List<String> evntTypeDespColumns = new ArrayList<String>();
        evntTypeDespColumns.add(MSS_EVNTTYPE_DESCRIPTION_COLUMN);
        subBIParameters.put(EVENT_DESCRIPTION_COLUMN_VAR, evntTypeDespColumns);
        subBIParameters.put(COLUMNS, FAILURE_COMMON_COLUMN);
        subBIParameters.put(TEMP_JOIN_TABLES, evntDespTable);
        subBIParameters.put(TEMP_JOIN_VIEW_VAR, EVENT_TYPE_TABLE_VIEW);
        subBIParameters.put(ON_CLAUSE_LEFT_OUTER_JOIN_ALIAS_TABLE, listOfTablesInJoin);
        return queryConsUtil.getQueryFromTemplate(MSS_LEFT_OUTER_JOIN_TEMPLATE, subBIParameters);
    }

    /**
     * This class is used hold query specific data used for different query types 
     * @author echchik
     *
     */
    protected class AllListInfoData {
        final List<String> listOfTablesInJoin = new ArrayList<String>();

        final List<String> listOfErrColumnAlias = new ArrayList<String>();

        final List<String> listOfSucColumnAlias = new ArrayList<String>();
    }

    /**
     * Used for JUNIT
     * @param queryConstructorUtils
     */
    public void setQueryConstructorUtils(final QueryConstructorUtils queryConstructorUtils) {
        this.queryConsUtil = queryConstructorUtils;
    }

    protected List<String> getJoinColumn() {
        final List<String> joinColumns = new ArrayList<String>();
        if (TYPE_IMSI.equals(type)) {
            joinColumns.add(TYPE_IMSI);
            return joinColumns;
        }
        joinColumns.add(TYPE_MSISDN);
        return joinColumns;
    }
}
