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
package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;
import javax.ejb.Stateless;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.errSucTotal.MssEventAnalysisErrSucTotalQueryCreator;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary.MssSmsAndLocationServiceSummaryQueryCreator;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary.MssVoiceServiceSummaryQueryCreator;

@Stateless
public class MssEventAnalysisQueryHelper {

    @EJB
    protected MssVoiceServiceSummaryQueryCreator mssVoiceServiceSummaryQueryCreator;

    @EJB
    protected MssEventAnalysisErrSucTotalQueryCreator mssEventAnalysisErrSucTotalQueryCreator;

    @EJB
    protected MssSmsAndLocationServiceSummaryQueryCreator mssSmsAndLocationServiceSummaryQueryCreator;

    public List<String> constructQuery(final Map<String, Object> templateParameters) {
        return getQuery(templateParameters);
    }

    private List<String> getQuery(final Map<String, Object> templateParameters) {
        if (isSummaryReport(templateParameters)) {
            final List<String> listOfQueries = new ArrayList<String>();
            boolean isOnlyVoice = false;
            if (templateParameters.containsKey(FAIL_TYPE)) {
                isOnlyVoice = true;
            }
            if (toAddVoiceQuery(templateParameters)) {
                listOfQueries.add(mssVoiceServiceSummaryQueryCreator.createQuery(templateParameters));
            }
            if (!isOnlyVoice) {
                if (toAddLocationServiceQuery(templateParameters)) {
                    mssSmsAndLocationServiceSummaryQueryCreator.setToCreateSmsQuery(false);
                    listOfQueries.add(mssSmsAndLocationServiceSummaryQueryCreator.createQuery(templateParameters));
                }
                if (toAddSmsQuery(templateParameters)) {
                    mssSmsAndLocationServiceSummaryQueryCreator.setToCreateSmsQuery(true);
                    listOfQueries.add(mssSmsAndLocationServiceSummaryQueryCreator.createQuery(templateParameters));
                }
            }
            return listOfQueries;
        } else if (isEventAnalysisReport(templateParameters)) {
            return mssEventAnalysisErrSucTotalQueryCreator.createQuery(templateParameters);
        }
        return null;
    }

    /**
     * This method checks whether the query should be created for a 
     * err,suc and total report to be provided
     * @return true if event analysis key is present else false
     */
    private boolean isEventAnalysisReport(final Map<String, Object> templateParameters) {
        final String key = (String) templateParameters.get(KEY_PARAM);
        if (KEY_TYPE_ERR.equals(key) || KEY_TYPE_SUC.equals(key) || KEY_TYPE_TOTAL.equals(key)) {
            return true;
        }
        return false;
    }

    /**
     * This method checks whether the query should be created for a 
     * summary report to be provided. For IMSI the summary report should
     * created only if the group name key is present 
     * @return true if summary key is present else false
     */
    private boolean isSummaryReport(final Map<String, Object> templateParameters) {
        final String key = (String) templateParameters.get(KEY_PARAM);
        if (KEY_TYPE_SUM.equals(key)) {
            return true;
        }
        return false;
    }

    /**
     * This method determines whether the voice summary query should be created
     * @param templateParameters map of data required for query generation
     * @return true if voice query to be created else false
     */
    private boolean toAddVoiceQuery(final Map<String, Object> templateParameters) {
        final String evntID = (String) templateParameters.get(EVENT_ID_PARAM);
        final boolean isInValidEvntId = StringUtils.isBlank(evntID);
        if (isInValidEvntId) {
            return true;
        } else if (!isInValidEvntId && templateParameters.containsKey(GROUP_NAME_KEY) && isMssVoiceEvent(evntID)) {
            return true;
        }
        return false;
    }

    /**
     * This method determines whether the location service summary query should be created
     * @param templateParameters map of data required for query generation
     * @return true if location service query to be created else false
     */
    private boolean toAddLocationServiceQuery(final Map<String, Object> templateParameters) {
        final String evntID = (String) templateParameters.get(EVENT_ID_PARAM);
        final boolean isInValidEvntId = StringUtils.isBlank(evntID);
        if (isInValidEvntId) {
            return true;
        } else if (!isInValidEvntId && templateParameters.containsKey(GROUP_NAME_KEY)
                && isMssLocationServiceEvent(evntID)) {
            return true;
        }
        return false;
    }

    /**
     * This method determines whether the SMS summary query should be created
     * @param templateParameters map of data required for query generation
     * @return true if SMS query to be created else false
     */
    private boolean toAddSmsQuery(final Map<String, Object> templateParameters) {
        final String evntID = (String) templateParameters.get(EVENT_ID_PARAM);
        final boolean isInValidEvntId = StringUtils.isBlank(evntID);
        if (isInValidEvntId) {
            return true;
        } else if (!isInValidEvntId && templateParameters.containsKey(GROUP_NAME_KEY) && isMssSMSEvent(evntID)) {
            return true;
        }
        return false;
    }

    public void setMssVoiceServiceSummaryQueryCreator(
            final MssVoiceServiceSummaryQueryCreator mssVoiceServiceSummaryQueryCreator) {
        this.mssVoiceServiceSummaryQueryCreator = mssVoiceServiceSummaryQueryCreator;
    }

    public void setMssEventAnalysisErrSucTotalQueryCreator(
            final MssEventAnalysisErrSucTotalQueryCreator mssEventAnalysisErrSucTotalQueryCreator) {
        this.mssEventAnalysisErrSucTotalQueryCreator = mssEventAnalysisErrSucTotalQueryCreator;
    }

    public void setMssSmsAndLocationServiceSummaryQueryCreator(
            final MssSmsAndLocationServiceSummaryQueryCreator mssSmsAndLocationServiceSummaryQueryCreator) {
        this.mssSmsAndLocationServiceSummaryQueryCreator = mssSmsAndLocationServiceSummaryQueryCreator;
    }
}
