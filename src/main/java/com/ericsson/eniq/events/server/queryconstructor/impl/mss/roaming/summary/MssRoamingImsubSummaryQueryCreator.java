/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.queryconstructor.impl.mss.roaming.summary;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.QueryConstructorConstants.*;

import javax.ejb.Stateless;

/**
 * @author egraman
 * @since 2011
 *
 */
@Stateless
public class MssRoamingImsubSummaryQueryCreator extends MssRoamingAnalysisSummaryQueryCreator {

    @Override
    protected String createSummaryQuery() {
        return createImsiTable();
    }

    private String createImsiTable() {
        // Voice tables
        roamingAnalysisParameters.put("errorTables", techPackTables.getErrTables(KEY_TYPE_ERR));
        roamingAnalysisParameters.put("dropCallTables", techPackTables.getErrTables(KEY_TYPE_DROP_CALL));
        roamingAnalysisParameters.put("sucTables", techPackTables.getSucTables(KEY_TYPE_SUC));
        // Location Service Tables
        roamingAnalysisParameters.put("errorLocTables", techPackTables.getErrTables(KEY_TYPE_LOC_SERVICE_ERR));
        roamingAnalysisParameters.put("sucLocTables", techPackTables.getSucTables(KEY_TYPE_LOC_SERVICE_SUC));
        // SMS Tables
        roamingAnalysisParameters.put("errorSmsTables", techPackTables.getErrTables(KEY_TYPE_SMS_ERR));
        roamingAnalysisParameters.put("sucSmsTables", techPackTables.getSucTables(KEY_TYPE_SMS_SUC));
        return queryConsUtil.getQueryFromTemplate(MSS_ROAMING_ANALYSIS_IMSI_TEMPLATE, roamingAnalysisParameters);
    }

}
