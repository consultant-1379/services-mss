/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.QueryConstructorConstants.MSS_SUMMARY_EVNT_ANALYSIS_VOICE_TABLE;

import java.util.ArrayList;
import java.util.List;

import javax.ejb.Stateless;

/**
 * This class provide the functionality to generate the query using location service schema
 * for supported node types
 * 
 * The Location service extends MssEventAnalysisSummaryQueryCreator class to override few method
 * to generate query for location service schema
 * @author echchik
 *
 */
@Stateless
public class MssSmsAndLocationServiceSummaryQueryCreator extends MssEventAnalysisSummaryQueryCreator {

    private boolean toCreateSmsQuery;

    public void setToCreateSmsQuery(final boolean toCreateSmsQuery) {
        this.toCreateSmsQuery = toCreateSmsQuery;
    }

    @Override
    protected String createNonGrpQuery(final AllListInfoData allListInfoData) {
        populateEventAnalysisParametersNonGrp();
        StringBuilder queryStr = createSmsOrLocationServiceQuery(allListInfoData);
        if (!isImsiOrMsisdnType()) {
            queryStr = getJoinOnTopology(queryStr, allListInfoData);
        }
        evntAnalysisParameters.put(ALL_TABLES_QUERY_PART, queryStr.toString());
        return queryConsUtil.getQueryFromTemplate(MSS_SUMMARY_EVNT_ANALYSIS_VOICE_TABLE, evntAnalysisParameters);
    }

    @Override
    protected String createGrpQuery(final AllListInfoData allListInfoData) {
        populateEventAnalysisParametersGrp();
        final StringBuilder queryStr = createSmsOrLocationServiceQuery(allListInfoData);
        evntAnalysisParameters.put(ALL_TABLES_QUERY_PART, queryStr.toString());
        return queryConsUtil.getQueryFromTemplate(MSS_SUMMARY_EVNT_ANALYSIS_VOICE_TABLE, evntAnalysisParameters);
    }

    @Override
    protected String createGrpSummaryDrilldownQuery(final AllListInfoData allListInfoData) {
        populateEventAnalysisParametersGrpSummaryDrilldownOnEvntId();
        StringBuilder queryStr = createSmsOrLocationServiceQuery(allListInfoData);
        queryStr = getJoinOnTopology(queryStr, allListInfoData);
        evntAnalysisParameters.put(ALL_TABLES_QUERY_PART, queryStr.toString());
        return queryConsUtil.getQueryFromTemplate(MSS_SUMMARY_EVNT_ANALYSIS_VOICE_TABLE, evntAnalysisParameters);
    }

    /**
     * This method returns query joining all tables required by both non group and group nodes
     * for location service table schema
     * @return query part representing the joined tables
     */
    private StringBuilder createSmsOrLocationServiceQuery(final AllListInfoData allListInfoData) {
        updateEventAnalysisParamsForSubBI();
        final StringBuilder queryStr = new StringBuilder();
        String onClause = "";
        final String sucTable = createSmsOrLocationServiceSuccessTable(allListInfoData,
                getTypeOfViewToUse(KEY_TYPE_SUC));
        queryStr.append(sucTable);
        appendFullOuterJoin(queryStr);
        final String errorView = createSmsOrLocationServiceErrTable(allListInfoData, getTypeOfViewToUse(KEY_TYPE_ERR));
        queryStr.append(errorView);
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
     * particular node type for location service
     * @return query part representing the success table for the given type
     */
    private String createSmsOrLocationServiceSuccessTable(final AllListInfoData allListInfoData,
            final TypesOfView viewToUse) {
        if (isAggregation) {
            updateFilterColumnsForSuccessAggregation();
            evntAnalysisParameters.put(TABLES_VAR, getAggTableList(viewToUse));
            evntAnalysisParameters.put(RAW_AGG_COLUMNS_VAR, SUM_OF_SUCCESSES_AGG);
        } else {
            updateFilterColumnsForRaw();
            evntAnalysisParameters.put(TABLES_VAR, getRawTableList(viewToUse));
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
     * This method will create the blocked table used to join the other table types for a
     * particular node type
     * @return query part representing the blocked table for the given type
     */
    private String createSmsOrLocationServiceErrTable(final AllListInfoData allListInfoData, final TypesOfView viewToUse) {
        if (isAggregation) {
            updateFilterColumnsForErrorAggregation();
            evntAnalysisParameters.put(TABLES_VAR, getAggTableList(viewToUse));
            evntAnalysisParameters.put(RAW_AGG_COLUMNS_VAR, SUM_OF_ERRORS_AGG);
        } else {
            updateFilterColumnsForRaw();
            evntAnalysisParameters.put(TABLES_VAR, getRawTableList(viewToUse));
            evntAnalysisParameters.put(RAW_AGG_COLUMNS_VAR, NO_OF_SUC_ERR_RAW);
        }
        evntAnalysisParameters.put(RAW_AGG_COLUMNS_ALIAS_VAR, NO_OF_ERRORS_AGG);
        evntAnalysisParameters.put(TABLES_ALIAS_VAR, ERROR_VIEW);
        allListInfoData.listOfErrColumnAlias.add(NO_OF_ERRORS_AGG);
        allListInfoData.listOfTablesInJoin.add(ERROR_VIEW);
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
        evntAnalysisParameters.put(TABLES_VAR, getRawTableList(getTypeOfViewToUse(KEY_TYPE_ERR)));
        final String templateToUse = getTemplateToUseForImsi(isGrpSummaryDrillDown);
        final String imsiQuery = queryConsUtil.getQueryFromTemplate(templateToUse, evntAnalysisParameters);
        evntAnalysisParameters.put(IMSI_VIEW, imsiQuery);
        evntAnalysisParameters.put(IMSI_TOTAL_VAR, IMPACTED_SUBSCRIBERS);
        imsiJoinTableList.add(IMPACTED_SUBSCRIBERS);
        final String template = getTemplateToUseForImsiTotal(isGrpSummaryDrillDown);
        return queryConsUtil.getQueryFromTemplate(template, evntAnalysisParameters);
    }

    /**
     * This method is used to fetch the type of view to use based on the tableType parameter and 
     * toCreateSmsQuery boolean class value
     * @param tableType represent the table type success or error
     * @return SMS error if toCreateSmsQuery is true and tableType is ERR
     *         LocServices error if toCreateSmsQuery is false and tableType is ERR
     *         SMS success if toCreateSmsQuery is true and tableType is SUC
     *         LocServices success if toCreateSmsQuery is false and tableType is SUC
     */
    private TypesOfView getTypeOfViewToUse(final String tableType) {
        if (KEY_TYPE_ERR.equals(tableType)) {
            if (toCreateSmsQuery) {
                return TypesOfView.SMS_ERROR;
            }
            return TypesOfView.LOC_SERVICE_ERROR;
        }
        if (toCreateSmsQuery) {
            return TypesOfView.SMS_SUCCESS;
        }
        return TypesOfView.LOC_SERVICE_SUCCESS;
    }
}
