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
public class MssSubscriberBISmsAndLocServiceSummaryQueryCreator extends MssSubscriberBISummaryQueryCreator {

    private boolean toCreateSmsSummaryQuery;

    /**
     * This method returns query joining all tables required by both non group and group nodes
     * @return query part representing the joined tables
     */
    @Override
    protected StringBuilder createQuery(final AllListInfoData allListInfoData) {
        final StringBuilder queryStr = new StringBuilder();
        String onClause = "";
        final String sucTable = createSuccessTable(allListInfoData);
        queryStr.append(sucTable);
        appendFullOuterJoin(queryStr);
        final String errorView = createErrorView(allListInfoData);
        queryStr.append(errorView);
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
    private String createSuccessTable(final AllListInfoData allListInfoData) {
        if (toCreateSmsSummaryQuery) {
            subBIParameters.put(TABLES_VAR, techPackTables.getSucTables(KEY_TYPE_SMS_SUC));
        } else {
            subBIParameters.put(TABLES_VAR, techPackTables.getSucTables(KEY_TYPE_LOC_SERVICE_SUC));
        }
        subBIParameters.put(RAW_AGG_COLUMNS_VAR, NO_OF_SUC_ERR_RAW);
        subBIParameters.put(RAW_AGG_COLUMNS_ALIAS_VAR, NO_OF_SUCCESSES_AGG);
        subBIParameters.put(TABLES_ALIAS_VAR, SUC_TABLE);
        allListInfoData.listOfTablesInJoin.add(SUC_TABLE);
        allListInfoData.listOfSucColumnAlias.add(NO_OF_SUCCESSES_AGG);
        final String templateToUse = MSS_SUBBI_ANALYSIS_RAW_VOICE_TEMPLATE;
        return queryConsUtil.getQueryFromTemplate(templateToUse, subBIParameters);
    }

    /**
     * This method will create the blocked table used to join the other table types for a
     * particular node type
     * @return query representing the error table for the given type
     */
    private String createErrorView(final AllListInfoData allListInfoData) {
        if (toCreateSmsSummaryQuery) {
            subBIParameters.put(TABLES_VAR, techPackTables.getErrTables(KEY_TYPE_SMS_ERR));
        } else {
            subBIParameters.put(TABLES_VAR, techPackTables.getErrTables(KEY_TYPE_LOC_SERVICE_ERR));
        }
        subBIParameters.put(RAW_AGG_COLUMNS_VAR, NO_OF_SUC_ERR_RAW);
        subBIParameters.put(RAW_AGG_COLUMNS_ALIAS_VAR, NO_OF_ERRORS_AGG);
        subBIParameters.put(TABLES_ALIAS_VAR, ERROR_VIEW);
        allListInfoData.listOfTablesInJoin.add(ERROR_VIEW);
        allListInfoData.listOfErrColumnAlias.add(NO_OF_ERRORS_AGG);
        final String templateToUse = MSS_SUBBI_ANALYSIS_RAW_VOICE_TEMPLATE;
        return queryConsUtil.getQueryFromTemplate(templateToUse, subBIParameters);
    }

    /**
     * This method will create the success table used for terminal analysis
     * @return query part representing the success table for the given type
     */
    private String createTerminalVoiceSuccessTable() {
        if (toCreateSmsSummaryQuery) {
            subBIParameters.put(TABLES_VAR, techPackTables.getSucTables(KEY_TYPE_SMS_SUC));
        } else {
            subBIParameters.put(TABLES_VAR, techPackTables.getSucTables(KEY_TYPE_LOC_SERVICE_SUC));
        }
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
        if (toCreateSmsSummaryQuery) {
            subBIParameters.put(TABLES_VAR, techPackTables.getErrTables(KEY_TYPE_SMS_ERR));
        } else {
            subBIParameters.put(TABLES_VAR, techPackTables.getErrTables(KEY_TYPE_LOC_SERVICE_ERR));
        }
        final List<String> rawColumns = new ArrayList<String>();
        rawColumns.add("count(*) as NO_OF_ERRORS");
        rawColumns.add("0 as NO_OF_SUCCESSES");
        subBIParameters.put(RAW_AGG_COLUMNS_VAR, rawColumns);
        return queryConsUtil.getQueryFromTemplate(MSS_SUBBI_TERMINAL_ANALYSIS_RAW_TEMPLATE, subBIParameters);
    }

    public void setToCreateSmsSummaryQuery(final boolean toCreateSmsSummaryQuery) {
        this.toCreateSmsSummaryQuery = toCreateSmsSummaryQuery;
    }
}
