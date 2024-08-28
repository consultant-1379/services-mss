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
public class MssRoamingVoiceServiceSummaryQueryCreator extends MssRoamingAnalysisSummaryQueryCreator {

    @Override
    protected String createSummaryQuery() {
        roamingAnalysisParameters.put("errorTable", createErrTable());
        roamingAnalysisParameters.put("sucTable", createSucTable());

        return queryConsUtil.getQueryFromTemplate(MSS_ROAMING_ANALYSIS_SUMMARY_TEMPLATE, roamingAnalysisParameters);
    }

    private String createErrTable() {
        roamingAnalysisParameters.put("errorTables", techPackTables.getErrTables());
        roamingAnalysisParameters.put("dropCallTables", techPackTables.getErrTables(KEY_TYPE_DROP_CALL));
        return queryConsUtil.getQueryFromTemplate(MSS_ROAMING_ANALYSIS_VOICE_ERR_TEMPLATE, roamingAnalysisParameters);
    }

    private String createSucTable() {
        roamingAnalysisParameters.put("sucTables", techPackTables.getSucTables(KEY_TYPE_SUC));
        return queryConsUtil.getQueryFromTemplate(MSS_ROAMING_ANALYSIS_SUC_TEMPLATE, roamingAnalysisParameters);
    }

}
