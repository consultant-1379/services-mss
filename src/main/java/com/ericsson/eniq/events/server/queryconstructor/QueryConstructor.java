/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.queryconstructor;

import java.util.List;
import java.util.Map;

import javax.ejb.EJB;
import javax.ejb.Stateless;

import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.MssEventAnalysisQueryHelper;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryHelper;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.roaming.MssRoamingAnalysisQueryHelper;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.subbi.MssSubscriberBIQueryHelper;

@Stateless
public class QueryConstructor {

    @EJB
    protected MssEventAnalysisQueryHelper mssEvntAnalysisQueryHelper;

    @EJB
    protected MssSubscriberBIQueryHelper mssSubscriberBIQueryHelper;

    @EJB
    protected MssEventVolumeQueryHelper mssEventVolumeQueryHelper;

    @EJB
    protected MssRoamingAnalysisQueryHelper mssRoamingAnalysisQueryHelper;

    /**
     * This method will return the list of MSS Event Analysis Query based on the template parameters value.
     * This method will return query for TAC,MANUFACTURER,MSC,CONTROLLER and CELL
     * for all time ranges and group(RAW, RAW GROUP, AGGREGATION and AGGREGATION GROUP)
     * a. Summary Report
     * b. Error, Success and Total report 
     */
    public List<String> getMssEventAnalysisQuery(final Map<String, Object> templateParameters) {
        return mssEvntAnalysisQueryHelper.constructQuery(templateParameters);
    }

    public List<String> getMssSubscriberBIFailuresQuery(final Map<String, Object> templateParameters) {
        return mssSubscriberBIQueryHelper.getFailureQuery(templateParameters);
    }

    public List<String> getMssSubscriberBIBusyHourQuery(final Map<String, Object> templateParameters) {
        return mssSubscriberBIQueryHelper.getBusyHourQuery(templateParameters);
    }

    public List<String> getMssSubscriberBIBusyDayQuery(final Map<String, Object> templateParameters) {
        return mssSubscriberBIQueryHelper.getBusyDayQuery(templateParameters);
    }

    public List<String> getMssSubscriberBICellAnalysisQuery(final Map<String, Object> templateParameters) {
        return mssSubscriberBIQueryHelper.getCellAnalysisQuery(templateParameters);
    }

    public List<String> getMssSubscriberBITerminalAnalysisQuery(final Map<String, Object> templateParameters) {
        return mssSubscriberBIQueryHelper.getTerminalAnalysisQuery(templateParameters);
    }

    public List<String> getMssSubscriberDetailsQuery(final Map<String, Object> templateParameters) {
        return mssSubscriberBIQueryHelper.getSubscriberDetailsQuery(templateParameters);
    }

    public List<String> getMssEventVolumeQuery(final Map<String, Object> templateParameters) {
        return mssEventVolumeQueryHelper.constructQuery(templateParameters);
    }

    public List<String> getMssRoamingAnalysisQuery(final Map<String, Object> templateParameters) {
        return mssRoamingAnalysisQueryHelper.constructQuery(templateParameters);
    }

    public void setMssEventAnalysisQueryHelper(final MssEventAnalysisQueryHelper mssEvntAnalysisQueryHelper) {
        this.mssEvntAnalysisQueryHelper = mssEvntAnalysisQueryHelper;
    }

    public void setMssSubscriberBIQueryHelper(final MssSubscriberBIQueryHelper mssSubscriberBIQueryHelper) {
        this.mssSubscriberBIQueryHelper = mssSubscriberBIQueryHelper;
    }

    public void setMssEventVolumeQueryHelper(final MssEventVolumeQueryHelper mssEventVolumeQueryHelper) {
        this.mssEventVolumeQueryHelper = mssEventVolumeQueryHelper;
    }

    public void setMssRoamingAnalysisQueryHelper(final MssRoamingAnalysisQueryHelper mssRoamingAnalysisQueryHelper) {
        this.mssRoamingAnalysisQueryHelper = mssRoamingAnalysisQueryHelper;
    }

}
