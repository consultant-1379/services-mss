/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.queryconstructor.impl.mss.roaming.summary;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;

import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.queryconstructor.QueryConstructorUtils;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary.MssSummaryTemplateInfoObject;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary.MssSummaryTemplateInfoToTypeMappings;

/**
 * @author egraman
 * @since 2011
 *
 */
public abstract class MssRoamingAnalysisSummaryQueryCreator {

    public static final int romaingAnalysisCount = 25;

    /*
     * This provides the MssSummaryTemplateInfoObject required for the type for which
     * query needs to be created
     */
    @EJB
    protected MssSummaryTemplateInfoToTypeMappings tempInfoToTypeMappings;

    /*
     * Holds the time range to be appended to agg view
     */
    protected String timeStringToAppend;

    /*
     * This object holds the data required by the templates to generate the query
     */
    protected MssSummaryTemplateInfoObject tempInfoForType;

    /*
     * Map to hold the parameters used to construct the query
     */
    protected Map<String, Object> roamingAnalysisParameters;

    /*
     * This object provide raw tables for blocked, dropped and success
     */
    protected TechPackTables techPackTables;

    /*
     * This parameter provides the type of roaming: 
     * ApplicationConstants.TYPE_ROAMING_COUNTRY or ApplicationConstants.TYPE_ROAMING_OPERATOR
     */
    protected String isOperatorOrCountry;

    /*
     * This utility is used to create query using template utilities
     */
    @EJB
    protected QueryConstructorUtils queryConsUtil;

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

    public String createQuery(final Map<String, Object> templateParameters) {
        final List<String> columnsToBeSelected = new ArrayList<String>();
        final List<String> columnsToBeSelectedImsi = new ArrayList<String>();

        roamingAnalysisParameters = new HashMap<String, Object>();
        roamingAnalysisParameters.putAll(templateParameters);

        techPackTables = (TechPackTables) roamingAnalysisParameters.get(TECH_PACK_TABLES);
        final String timeRange = roamingAnalysisParameters.get(TIMERANGE_PARAM).toString();
        timeStringToAppend = returnAggregateViewTypeWithOutOneMinute(timeRange);

        isOperatorOrCountry = roamingAnalysisParameters.get(ROAMING_OBJECT).toString();
        	
        columnsToBeSelected.add(ROAMING_IMSI_MCC_PARAM);
        columnsToBeSelectedImsi.add(ROAMING_IMSI_MCC_PARAM);
        if (TYPE_ROAMING_OPERATOR.equals(isOperatorOrCountry)) {
        	columnsToBeSelected.add(ROAMING_IMSI_MNC_PARAM);
            columnsToBeSelectedImsi.add(ROAMING_IMSI_MNC_PARAM);
        	}
        columnsToBeSelectedImsi.add(IMSI);

        roamingAnalysisParameters.put("columnsToBeSelected", columnsToBeSelected);
        roamingAnalysisParameters.put("columnsToBeSelectedImsi", columnsToBeSelectedImsi);
        roamingAnalysisParameters.put("isOpOrCountry", isOperatorOrCountry);
        roamingAnalysisParameters.put("romaingAnalysisCount", romaingAnalysisCount);

        roamingAnalysisParameters.put("op_or_country", romaingAnalysisCount);

        return createSummaryQuery().toString();
    }

    protected abstract String createSummaryQuery();

}
