/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.queryconstructor.impl.mss.subbi;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;
import javax.ejb.Stateless;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.errSucTotal.MssEventAnalysisErrSucTotalQueryCreator;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary.MssSmsAndLocationServiceSummaryQueryCreator;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary.MssVoiceServiceSummaryQueryCreator;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.subbi.summary.MssSubscriberBISmsAndLocServiceSummaryQueryCreator;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.subbi.summary.MssSubscriberBIVoiceSummaryQueryCreator;

@Stateless
public class MssSubscriberBIQueryHelper {

    @EJB
    protected MssSubscriberBIVoiceSummaryQueryCreator mssSubscriberBIVoiceSummaryQueryCreator;

    @EJB
    protected MssSubscriberBISmsAndLocServiceSummaryQueryCreator mssSubscriberBISmsAndLocServiceSummaryQueryCreator;

    @EJB
    protected MssEventAnalysisErrSucTotalQueryCreator mssEventAnalysisErrSucTotalQueryCreator;

    @EJB
    protected MssVoiceServiceSummaryQueryCreator mssVoiceServiceSummaryQueryCreator;

    @EJB
    protected MssSmsAndLocationServiceSummaryQueryCreator mssSmsAndLocationServiceSummaryQueryCreator;

    public List<String> getFailureQuery(final Map<String, Object> templateParameters) {
        if (templateParameters.containsKey(EVENT_NAME_PARAM)) {
            return mssEventAnalysisErrSucTotalQueryCreator.createQuery(templateParameters);
        }
        final List<String> allQueries = new ArrayList<String>();
        allQueries.addAll(mssSubscriberBIVoiceSummaryQueryCreator.createFailureQuery(templateParameters));
        mssSubscriberBISmsAndLocServiceSummaryQueryCreator.setToCreateSmsSummaryQuery(false);
        allQueries.addAll(mssSubscriberBISmsAndLocServiceSummaryQueryCreator.createFailureQuery(templateParameters));
        mssSubscriberBISmsAndLocServiceSummaryQueryCreator.setToCreateSmsSummaryQuery(true);
        allQueries.addAll(mssSubscriberBISmsAndLocServiceSummaryQueryCreator.createFailureQuery(templateParameters));
        return allQueries;
    }

    public List<String> getBusyHourQuery(final Map<String, Object> templateParameters) {
        if (isSummaryReport(templateParameters)) {
            return getSubbiEventAnalysisQueries(templateParameters);
        } else if (isEventAnalysisReport(templateParameters)) {
            return mssEventAnalysisErrSucTotalQueryCreator.createQuery(templateParameters);
        }
        final List<String> allQueries = new ArrayList<String>();
        allQueries.addAll(mssSubscriberBIVoiceSummaryQueryCreator.createBusyHourQuery(templateParameters));
        mssSubscriberBISmsAndLocServiceSummaryQueryCreator.setToCreateSmsSummaryQuery(false);
        allQueries.addAll(mssSubscriberBISmsAndLocServiceSummaryQueryCreator.createBusyHourQuery(templateParameters));
        mssSubscriberBISmsAndLocServiceSummaryQueryCreator.setToCreateSmsSummaryQuery(true);
        allQueries.addAll(mssSubscriberBISmsAndLocServiceSummaryQueryCreator.createBusyHourQuery(templateParameters));
        return allQueries;
    }

    public List<String> getBusyDayQuery(final Map<String, Object> templateParameters) {
        if (isSummaryReport(templateParameters)) {
            return getSubbiEventAnalysisQueries(templateParameters);
        } else if (isEventAnalysisReport(templateParameters)) {
            return mssEventAnalysisErrSucTotalQueryCreator.createQuery(templateParameters);
        }
        final List<String> allQueries = new ArrayList<String>();
        allQueries.addAll(mssSubscriberBIVoiceSummaryQueryCreator.createBusyDayQuery(templateParameters));
        mssSubscriberBISmsAndLocServiceSummaryQueryCreator.setToCreateSmsSummaryQuery(false);
        allQueries.addAll(mssSubscriberBISmsAndLocServiceSummaryQueryCreator.createBusyDayQuery(templateParameters));
        mssSubscriberBISmsAndLocServiceSummaryQueryCreator.setToCreateSmsSummaryQuery(true);
        allQueries.addAll(mssSubscriberBISmsAndLocServiceSummaryQueryCreator.createBusyDayQuery(templateParameters));
        return allQueries;
    }

    public List<String> getCellAnalysisQuery(final Map<String, Object> templateParameters) {
        if (isSummaryReport(templateParameters)) {
            return getSubbiEventAnalysisQueries(templateParameters);
        } else if (isEventAnalysisReport(templateParameters)) {
            return mssEventAnalysisErrSucTotalQueryCreator.createQuery(templateParameters);
        }
        final List<String> allQueries = new ArrayList<String>();
        allQueries.addAll(mssSubscriberBIVoiceSummaryQueryCreator.createCellAnalysisQuery(templateParameters));
        mssSubscriberBISmsAndLocServiceSummaryQueryCreator.setToCreateSmsSummaryQuery(false);
        allQueries.addAll(mssSubscriberBISmsAndLocServiceSummaryQueryCreator
                .createCellAnalysisQuery(templateParameters));
        mssSubscriberBISmsAndLocServiceSummaryQueryCreator.setToCreateSmsSummaryQuery(true);
        allQueries.addAll(mssSubscriberBISmsAndLocServiceSummaryQueryCreator
                .createCellAnalysisQuery(templateParameters));
        return allQueries;
    }

    public List<String> getTerminalAnalysisQuery(final Map<String, Object> templateParameters) {
        if (templateParameters.containsKey(TAC_PARAM)) {
            return mssEventAnalysisErrSucTotalQueryCreator.createQuery(templateParameters);
        }
        final List<String> allQueries = new ArrayList<String>();
        allQueries.addAll(mssSubscriberBIVoiceSummaryQueryCreator.createTerminalAnalysisQuery(templateParameters));
        mssSubscriberBISmsAndLocServiceSummaryQueryCreator.setToCreateSmsSummaryQuery(false);
        allQueries.addAll(mssSubscriberBISmsAndLocServiceSummaryQueryCreator
                .createTerminalAnalysisQuery(templateParameters));
        mssSubscriberBISmsAndLocServiceSummaryQueryCreator.setToCreateSmsSummaryQuery(true);
        allQueries.addAll(mssSubscriberBISmsAndLocServiceSummaryQueryCreator
                .createTerminalAnalysisQuery(templateParameters));
        return allQueries;
    }

    private List<String> getSubbiEventAnalysisQueries(final Map<String, Object> templateParameters) {
        final List<String> allQueries = new ArrayList<String>();

        if (toAddVoiceQuery(templateParameters)) {
            allQueries.add(mssVoiceServiceSummaryQueryCreator.createQuery(templateParameters));
        }
        if (toAddLocationServiceQuery(templateParameters)) {
            mssSmsAndLocationServiceSummaryQueryCreator.setToCreateSmsQuery(false);
            allQueries.add(mssSmsAndLocationServiceSummaryQueryCreator.createQuery(templateParameters));
        }
        if (toAddSmsQuery(templateParameters)) {
            mssSmsAndLocationServiceSummaryQueryCreator.setToCreateSmsQuery(true);
            allQueries.add(mssSmsAndLocationServiceSummaryQueryCreator.createQuery(templateParameters));
        }
        return allQueries;
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
        }
        return false;
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

    public List<String> getSubscriberDetailsQuery(final Map<String, Object> templateParameters) {
        return mssEventAnalysisErrSucTotalQueryCreator.createSubcriberDetailsQuery(templateParameters);
    }

    public void setMssSubscriberBIVoiceSummaryQueryCreator(
            final MssSubscriberBIVoiceSummaryQueryCreator mssSubscriberBIVoiceSummaryQueryCreator) {
        this.mssSubscriberBIVoiceSummaryQueryCreator = mssSubscriberBIVoiceSummaryQueryCreator;
    }

    public void setMssEventAnalysisErrSucTotalQueryCreator(
            final MssEventAnalysisErrSucTotalQueryCreator mssEventAnalysisErrSucTotalQueryCreator) {
        this.mssEventAnalysisErrSucTotalQueryCreator = mssEventAnalysisErrSucTotalQueryCreator;
    }

    public void setMssSubscriberBISmsAndLocServiceSummaryQueryCreator(
            final MssSubscriberBISmsAndLocServiceSummaryQueryCreator mssSubscriberBISmsAndLocServiceSummaryQueryCreator) {
        this.mssSubscriberBISmsAndLocServiceSummaryQueryCreator = mssSubscriberBISmsAndLocServiceSummaryQueryCreator;
    }

    public void setMssVoiceServiceSummaryQueryCreator(
            final MssVoiceServiceSummaryQueryCreator mssVoiceServiceSummaryQueryCreator) {
        this.mssVoiceServiceSummaryQueryCreator = mssVoiceServiceSummaryQueryCreator;
    }

    public void setMssSmsAndLocationServiceSummaryQueryCreator(
            final MssSmsAndLocationServiceSummaryQueryCreator mssSmsAndLocationServiceSummaryQueryCreator) {
        this.mssSmsAndLocationServiceSummaryQueryCreator = mssSmsAndLocationServiceSummaryQueryCreator;
    }
}
