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
public class MssRoamingSmsAndLocationServiceSummaryQueryCreator extends MssRoamingAnalysisSummaryQueryCreator {

    private boolean toCreateSmsQuery;

    public void setToCreateSmsQuery(final boolean toCreateSmsQuery) {
        this.toCreateSmsQuery = toCreateSmsQuery;
    }

    @Override
    protected String createSummaryQuery() {
        roamingAnalysisParameters.put("errorTable", createErrTable());
        roamingAnalysisParameters.put("sucTable", createSucTable());

        return queryConsUtil.getQueryFromTemplate(MSS_ROAMING_ANALYSIS_SUMMARY_TEMPLATE, roamingAnalysisParameters);
    }

    private String createErrTable() {
        if (toCreateSmsQuery) {
            roamingAnalysisParameters.put("errorTables", techPackTables.getErrTables(KEY_TYPE_SMS_ERR));
        } else {
            roamingAnalysisParameters.put("errorTables", techPackTables.getErrTables(KEY_TYPE_LOC_SERVICE_ERR));
        }
        return queryConsUtil.getQueryFromTemplate(MSS_ROAMING_ANALYSIS_ERR_TEMPLATE, roamingAnalysisParameters);
    }

    private String createSucTable() {
        if (toCreateSmsQuery) {
            roamingAnalysisParameters.put("sucTables", techPackTables.getSucTables(KEY_TYPE_SMS_SUC));
        } else {
            roamingAnalysisParameters.put("sucTables", techPackTables.getSucTables(KEY_TYPE_LOC_SERVICE_SUC));
        }
        return queryConsUtil.getQueryFromTemplate(MSS_ROAMING_ANALYSIS_SUC_TEMPLATE, roamingAnalysisParameters);
    }

}
