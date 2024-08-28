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
import static com.ericsson.eniq.events.server.queryconstructor.QueryConstructorConstants.MSS_SUMMARY_EVNT_ANALYSIS_VOICE_TABLE;

import java.util.ArrayList;
import java.util.List;

import javax.ejb.Stateless;

/**
 * This class provide the functionality to generate the query using voice service schema
 * for supported node types
 * 
 * The Voice service extends MssEventAnalysisSummaryQueryCreator class to override few method 
 * to generate query for voice service schema
 * @author echchik
 *
 */
@Stateless
public class MssVoiceServiceSummaryQueryCreator extends MssEventAnalysisSummaryQueryCreator {

    @Override
    protected String createNonGrpQuery(final AllListInfoData allListInfoData) {
        populateEventAnalysisParametersNonGrp();
        StringBuilder queryStr = createVoiceQuery(allListInfoData);
        if (!isImsiOrMsisdnType()) {
            queryStr = getJoinOnTopology(queryStr, allListInfoData);
        }
        evntAnalysisParameters.put(ALL_TABLES_QUERY_PART, queryStr.toString());
        return queryConsUtil.getQueryFromTemplate(MSS_SUMMARY_EVNT_ANALYSIS_VOICE_TABLE, evntAnalysisParameters);
    }

    @Override
    protected String createGrpQuery(final AllListInfoData allListInfoData) {
        populateEventAnalysisParametersGrp();
        final StringBuilder queryStr = createVoiceQuery(allListInfoData);
        evntAnalysisParameters.put(ALL_TABLES_QUERY_PART, queryStr.toString());
        return queryConsUtil.getQueryFromTemplate(MSS_SUMMARY_EVNT_ANALYSIS_VOICE_TABLE, evntAnalysisParameters);
    }

    @Override
    protected String createGrpSummaryDrilldownQuery(final AllListInfoData allListInfoData) {
        populateEventAnalysisParametersGrpSummaryDrilldownOnEvntId();
        StringBuilder queryStr = createVoiceQuery(allListInfoData);
        queryStr = getJoinOnTopology(queryStr, allListInfoData);
        evntAnalysisParameters.put(ALL_TABLES_QUERY_PART, queryStr.toString());
        return queryConsUtil.getQueryFromTemplate(MSS_SUMMARY_EVNT_ANALYSIS_VOICE_TABLE, evntAnalysisParameters);
    }

    /**
     * This method returns query joining all tables required by both non group and group nodes
     * @return query part representing the joined tables
     */
    private StringBuilder createVoiceQuery(final AllListInfoData allListInfoData) {
        updateEventAnalysisParamsForSubBI();
        final StringBuilder queryStr = new StringBuilder();
        String onClause = "";
        final String sucTable = createVoiceSuccessTable(allListInfoData);
        queryStr.append(sucTable);
        appendFullOuterJoin(queryStr);
        final String combineErrorView = createCombinedErrorView(allListInfoData);
        queryStr.append(combineErrorView);
        onClause = getOnClause(allListInfoData.listOfTablesInJoin, allListInfoData.isGrpSummaryDrillDown);
        queryStr.append(onClause);

        allListInfoData.listOfAllColumnAlias.addAll(allListInfoData.listOfSucColumnAlias);
        allListInfoData.listOfAllColumnAlias.addAll(allListInfoData.listOfErrColumnAlias);

        if (!isImsiNonGroup() && !TYPE_MSISDN.equals(type)) {
            appendFullOuterJoin(queryStr);
            final List<String> imsiJoinTableList = new ArrayList<String>();
            final String imsiTable = createImsiTable(imsiJoinTableList, allListInfoData.isGrpSummaryDrillDown);
            imsiJoinTableList.addAll(allListInfoData.listOfTablesInJoin);
            queryStr.append(imsiTable);
            onClause = getOnClause(imsiJoinTableList, allListInfoData.isGrpSummaryDrillDown);
            queryStr.append(onClause);
        }

        final String leftJoinEvntTable = createLeftJoinWithEvntDespTable(allListInfoData.listOfTablesInJoin);
        queryStr.append(leftJoinEvntTable);

        return queryStr;
    }

    /**
     * This method will create the success table used to join the other table types for a
     * particular node type
     * @return query part representing the success table for the given type
     */
    private String createVoiceSuccessTable(final AllListInfoData allListInfoData) {
        if (isAggregation) {
            updateFilterColumnsForSuccessAggregation();
            evntAnalysisParameters.put(TABLES_VAR, getAggTableList(TypesOfView.SUCCESS));
            evntAnalysisParameters.put(RAW_AGG_COLUMNS_VAR, SUM_OF_SUCCESSES_AGG);
        } else {
            updateFilterColumnsForRaw();
            evntAnalysisParameters.put(TABLES_VAR, getRawTableList(TypesOfView.SUCCESS));
            evntAnalysisParameters.put(RAW_AGG_COLUMNS_VAR, NO_OF_SUC_ERR_RAW);
        }
        evntAnalysisParameters.put(RAW_AGG_COLUMNS_ALIAS_VAR, NO_OF_SUCCESSES_AGG);
        evntAnalysisParameters.put(TABLES_ALIAS_VAR, SUC_TABLE);
        allListInfoData.listOfTablesInJoin.add(SUC_TABLE);
        allListInfoData.listOfSucColumnAlias.add(NO_OF_SUCCESSES_AGG);
        final String templateToUse = getTemplateToUseForErrNSucTables(allListInfoData.isGrpSummaryDrillDown);
        return queryConsUtil.getQueryFromTemplate(templateToUse, evntAnalysisParameters);
    }

    /**
     * This method is used to combine both the blocked and dropped table as error view
     * @param allListInfoData list that hold all data needed for creating final and sub queries 
     * @return the sub query representing error view
     */
    private String createCombinedErrorView(final AllListInfoData allListInfoData) {
        final List<String> allErrTableAlias = new ArrayList<String>();
        final StringBuilder combineErrView = new StringBuilder();
        if (allListInfoData.isftype) {
            if (allListInfoData.FAIL_TYPE.contains(BLOCKED_VALUE)) {
                final String voiceBlockedTable = createVoiceBlockedTable(allListInfoData, allErrTableAlias);
                combineErrView.append(voiceBlockedTable);
            }
            if (allListInfoData.FAIL_TYPE.contains(DROPPED_VALUE)) {
                final String voiceDroppedTable = createVoiceDroppedTable(allListInfoData, allErrTableAlias);
                combineErrView.append(voiceDroppedTable);
            }
        } else {
            final String voiceBlockedTable = createVoiceBlockedTable(allListInfoData, allErrTableAlias);
            combineErrView.append(voiceBlockedTable);
            appendFullOuterJoin(combineErrView);
            final String voiceDroppedTable = createVoiceDroppedTable(allListInfoData, allErrTableAlias);
            combineErrView.append(voiceDroppedTable);
            final String onClause = getOnClause(allErrTableAlias, allListInfoData.isGrpSummaryDrillDown);
            combineErrView.append(onClause);
        }
        allListInfoData.listOfTablesInJoin.add(ERROR_VIEW);
        evntAnalysisParameters.put(ERROR_VIEW, ERROR_VIEW);
        evntAnalysisParameters.put(COMBINED_ERROR_TABLE, combineErrView.toString());
        evntAnalysisParameters.put(LIST_OF_ERROR_TABLE_ALIAS, allErrTableAlias);
        final String template = getTemplateToUseForCombinedErrView(allListInfoData.isGrpSummaryDrillDown);
        return queryConsUtil.getQueryFromTemplate(template, evntAnalysisParameters);
    }

    /**
     * This method will create the blocked table used to join the other table types for a
     * particular node type
     * @return query part representing the blocked table for the given type
     */
    private String createVoiceBlockedTable(final AllListInfoData allListInfoData, final List<String> allErrTableAlias) {
        if (isAggregation) {
            updateFilterColumnsForErrorAggregation();
            evntAnalysisParameters.put(TABLES_VAR, getAggTableList(TypesOfView.BLOCKED));
            evntAnalysisParameters.put(RAW_AGG_COLUMNS_VAR, SUM_OF_ERRORS_AGG);
        } else {
            updateFilterColumnsForRaw();
            evntAnalysisParameters.put(TABLES_VAR, getRawTableList(TypesOfView.BLOCKED));
            evntAnalysisParameters.put(RAW_AGG_COLUMNS_VAR, NO_OF_SUC_ERR_RAW);
        }
        evntAnalysisParameters.put(RAW_AGG_COLUMNS_ALIAS_VAR, NO_OF_BLOCKED_CALLS);
        evntAnalysisParameters.put(TABLES_ALIAS_VAR, ERR_AS_TABLE_BLOCKED);
        allErrTableAlias.add(ERR_AS_TABLE_BLOCKED);
        allListInfoData.listOfErrColumnAlias.add(NO_OF_BLOCKED_CALLS);
        final String templateToUse = getTemplateToUseForErrNSucTables(allListInfoData.isGrpSummaryDrillDown);
        return queryConsUtil.getQueryFromTemplate(templateToUse, evntAnalysisParameters);
    }

    /**
     * This method will create the dropped table used to join the other table types for a
     * particular node type
     * @return query part representing the dropped table for the given type
     */
    private String createVoiceDroppedTable(final AllListInfoData allListInfoData, final List<String> allErrTableAlias) {
        if (isAggregation) {
            updateFilterColumnsForErrorAggregation();
            evntAnalysisParameters.put(TABLES_VAR, getAggTableList(TypesOfView.DROPPED));
            evntAnalysisParameters.put(RAW_AGG_COLUMNS_VAR, SUM_OF_ERRORS_AGG);
        } else {
            updateFilterColumnsForRaw();
            evntAnalysisParameters.put(TABLES_VAR, getRawTableList(TypesOfView.DROPPED));
            evntAnalysisParameters.put(RAW_AGG_COLUMNS_VAR, NO_OF_SUC_ERR_RAW);
        }
        evntAnalysisParameters.put(RAW_AGG_COLUMNS_ALIAS_VAR, NO_OF_DROPPED_CALLS);
        evntAnalysisParameters.put(TABLES_ALIAS_VAR, ERR_AS_TABLE_DROPPED);
        allErrTableAlias.add(ERR_AS_TABLE_DROPPED);
        allListInfoData.listOfErrColumnAlias.add(NO_OF_DROPPED_CALLS);
        final String templateToUse = getTemplateToUseForErrNSucTables(allListInfoData.isGrpSummaryDrillDown);
        return queryConsUtil.getQueryFromTemplate(templateToUse, evntAnalysisParameters);
    }

    @Override
    protected String createImsiTable(final List<String> imsiJoinTableList, final boolean isGrpSummaryDrillDown) {
        //As Imsi will always go to raw table always TAC exclusion condition should be used
        //only condition where TAC exclusion condition is not used if the query is for a 
        //Exclusive_TAC group or drill down from an Exclusive_TAC group summary
        //IMSI table should be joined at the end
        if (!isExcludedTacOrGroup) {
            evntAnalysisParameters.put(USE_TAC_EXCLUSION_BOOLEAN, true);
        }
        final StringBuilder imsiView = new StringBuilder(getBlockedImsitable(isGrpSummaryDrillDown));
        appendUnionAll(imsiView);
        imsiView.append(getDroppedImsiTable(isGrpSummaryDrillDown));
        evntAnalysisParameters.put(IMSI_VIEW, imsiView.toString());
        evntAnalysisParameters.put(IMSI_TOTAL_VAR, IMPACTED_SUBSCRIBERS);
        imsiJoinTableList.add(IMPACTED_SUBSCRIBERS);
        final String template = getTemplateToUseForImsiTotal(isGrpSummaryDrillDown);
        return queryConsUtil.getQueryFromTemplate(template, evntAnalysisParameters);
    }

    /**
     * This method will return the blocked table imsi rows used to fetch impacted subscribers data
     * @return return query part of blocked table imsi
     */
    private String getBlockedImsitable(final boolean isGrpSummaryDrillDown) {
        evntAnalysisParameters.put(TABLES_VAR, getRawTableList(TypesOfView.BLOCKED));
        final String templateToUse = getTemplateToUseForImsi(isGrpSummaryDrillDown);
        return queryConsUtil.getQueryFromTemplate(templateToUse, evntAnalysisParameters);
    }

    /**
     * This method will return the dropped table imsi rows used to fetch impacted subscribers data
     * @return return query part of dropped table imsi
     */
    private String getDroppedImsiTable(final boolean isGrpSummaryDrillDown) {
        evntAnalysisParameters.put(TABLES_VAR, getRawTableList(TypesOfView.DROPPED));
        final String templateToUse = getTemplateToUseForImsi(isGrpSummaryDrillDown);
        return queryConsUtil.getQueryFromTemplate(templateToUse, evntAnalysisParameters);
    }
}
