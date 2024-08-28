/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.queryconstructor.impl.mss.subbi.summary;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.QueryConstructorConstants.*;

import java.util.ArrayList;
import java.util.List;

import javax.ejb.Stateless;

@Stateless
public class MssSubscriberBIVoiceSummaryQueryCreator extends MssSubscriberBISummaryQueryCreator {

    /**
     * This method returns query joining all tables required by both non group and group nodes
     * @return query part representing the joined tables
     */
    @Override
    protected StringBuilder createQuery(final AllListInfoData allListInfoData) {
        final StringBuilder queryStr = new StringBuilder();
        String onClause = "";
        final String sucTable = createVoiceSuccessTable(allListInfoData);
        queryStr.append(sucTable);
        appendFullOuterJoin(queryStr);
        final String combineErrorView = createCombinedErrorView(allListInfoData);
        queryStr.append(combineErrorView);
        onClause = getOnClause(allListInfoData.listOfTablesInJoin);
        queryStr.append(onClause);
        return queryStr;
    }

    @Override
    protected StringBuilder createTerminalAnalysisQuery(final AllListInfoData allListInfoData) {
        final StringBuilder queryStr = new StringBuilder();
        final String sucTable = createTerminalVoiceSuccessTable();
        queryStr.append(sucTable);
        queryStr.append("\n");
        queryStr.append(UNION_ALL);
        queryStr.append("\n");
        final String combineErrorView = createTerminalVoiceErrTable();
        queryStr.append(combineErrorView);
        return queryStr;
    }

    /**
     * This method will create the success table used to join the other table types for a
     * particular subbi analysis type
     * @return query part representing the success table for the given type
     */
    private String createVoiceSuccessTable(final AllListInfoData allListInfoData) {
        subBIParameters.put(TABLES_VAR, techPackTables.getSucTables());
        subBIParameters.put(RAW_AGG_COLUMNS_VAR, NO_OF_SUC_ERR_RAW);
        subBIParameters.put(RAW_AGG_COLUMNS_ALIAS_VAR, NO_OF_SUCCESSES_AGG);
        subBIParameters.put(TABLES_ALIAS_VAR, SUC_TABLE);
        allListInfoData.listOfTablesInJoin.add(SUC_TABLE);
        allListInfoData.listOfSucColumnAlias.add(NO_OF_SUCCESSES_AGG);
        return queryConsUtil.getQueryFromTemplate(MSS_SUBBI_ANALYSIS_RAW_VOICE_TEMPLATE, subBIParameters);
    }

    /**
     * This method is used to combine both the blocked and dropped table as error view
     * @param allListInfoData list that hold all data needed for creating final and sub queries 
     * @return the sub query representing error view
     */
    private String createCombinedErrorView(final AllListInfoData allListInfoData) {
        final List<String> allErrTableAlias = new ArrayList<String>();
        final StringBuilder combineErrView = new StringBuilder();
        final String voiceBlockedTable = createVoiceBlockedTable(allListInfoData, allErrTableAlias);
        combineErrView.append(voiceBlockedTable);
        appendFullOuterJoin(combineErrView);
        final String voiceDroppedTable = createVoiceDroppedTable(allListInfoData, allErrTableAlias);
        combineErrView.append(voiceDroppedTable);
        final String onClause = getOnClause(allErrTableAlias);
        combineErrView.append(onClause);
        allListInfoData.listOfTablesInJoin.add(ERROR_VIEW);
        subBIParameters.put(ERROR_VIEW, ERROR_VIEW);
        subBIParameters.put(COMBINED_ERROR_TABLE, combineErrView.toString());
        subBIParameters.put(LIST_OF_ERROR_TABLE_ALIAS, allErrTableAlias);
        return queryConsUtil.getQueryFromTemplate(MSS_SUBBI_ANALYSIS_ERR_VIEW_VOICE_TEMPLATE, subBIParameters);
    }

    /**
     * This method will create the blocked table used to join the other table types for a
     * particular node type
     * @return query part representing the blocked table for the given type
     */
    private String createVoiceBlockedTable(final AllListInfoData allListInfoData, final List<String> allErrTableAlias) {
        subBIParameters.put(TABLES_VAR, techPackTables.getErrTables(KEY_TYPE_ERR));
        subBIParameters.put(RAW_AGG_COLUMNS_VAR, NO_OF_SUC_ERR_RAW);
        subBIParameters.put(RAW_AGG_COLUMNS_ALIAS_VAR, NO_OF_BLOCKED_CALLS);
        subBIParameters.put(TABLES_ALIAS_VAR, ERR_AS_TABLE_BLOCKED);
        allErrTableAlias.add(ERR_AS_TABLE_BLOCKED);
        allListInfoData.listOfErrColumnAlias.add(NO_OF_BLOCKED_CALLS);
        return queryConsUtil.getQueryFromTemplate(MSS_SUBBI_ANALYSIS_RAW_VOICE_TEMPLATE, subBIParameters);
    }

    /**
     * This method will create the dropped table used to join the other table types for a
     * particular subbi analysis type
     * @return query part representing the dropped table for the given type
     */
    private String createVoiceDroppedTable(final AllListInfoData allListInfoData, final List<String> allErrTableAlias) {
        subBIParameters.put(TABLES_VAR, techPackTables.getErrTables(KEY_TYPE_DROP_CALL));
        subBIParameters.put(RAW_AGG_COLUMNS_VAR, NO_OF_SUC_ERR_RAW);
        subBIParameters.put(RAW_AGG_COLUMNS_ALIAS_VAR, NO_OF_DROPPED_CALLS);
        subBIParameters.put(TABLES_ALIAS_VAR, ERR_AS_TABLE_DROPPED);
        allErrTableAlias.add(ERR_AS_TABLE_DROPPED);
        allListInfoData.listOfErrColumnAlias.add(NO_OF_DROPPED_CALLS);
        return queryConsUtil.getQueryFromTemplate(MSS_SUBBI_ANALYSIS_RAW_VOICE_TEMPLATE, subBIParameters);
    }

    /**
     * This method will create the success table used for terminal analysis
     * @return query part representing the success table for the given type
     */
    private String createTerminalVoiceSuccessTable() {
        subBIParameters.put(TABLES_VAR, techPackTables.getSucTables());
        final List<String> rawColumns = new ArrayList<String>();
        rawColumns.add("0 as NO_OF_ERRORS");
        rawColumns.add("count(*) as NO_OF_SUCCESSES");
        subBIParameters.put(RAW_AGG_COLUMNS_VAR, rawColumns);
        return queryConsUtil.getQueryFromTemplate(MSS_SUBBI_TERMINAL_ANALYSIS_RAW_TEMPLATE, subBIParameters);
    }

    /**
     * This method will create the success table used for terminal analysis
     * @return query part representing the blocked table for the given type
     */
    private String createTerminalVoiceErrTable() {
        final List<String> errTables = new ArrayList<String>();
        errTables.addAll(techPackTables.getErrTables(KEY_TYPE_ERR));
        errTables.addAll(techPackTables.getErrTables(KEY_TYPE_DROP_CALL));
        subBIParameters.put(TABLES_VAR, errTables);
        final List<String> rawColumns = new ArrayList<String>();
        rawColumns.add("count(*) as NO_OF_ERRORS");
        rawColumns.add("0 as NO_OF_SUCCESSES");
        subBIParameters.put(RAW_AGG_COLUMNS_VAR, rawColumns);
        return queryConsUtil.getQueryFromTemplate(MSS_SUBBI_TERMINAL_ANALYSIS_RAW_TEMPLATE, subBIParameters);
    }
}
