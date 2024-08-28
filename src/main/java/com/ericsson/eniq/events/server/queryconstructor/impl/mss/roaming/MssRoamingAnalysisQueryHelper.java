/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.queryconstructor.impl.mss.roaming;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;
import javax.ejb.Stateless;

import com.ericsson.eniq.events.server.queryconstructor.impl.mss.roaming.summary.MssRoamingImsubSummaryQueryCreator;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.roaming.summary.MssRoamingSmsAndLocationServiceSummaryQueryCreator;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.roaming.summary.MssRoamingVoiceServiceSummaryQueryCreator;

/**
 * @author egraman
 * @since 2011
 *
 */
@Stateless
public class MssRoamingAnalysisQueryHelper {

    @EJB
    protected MssRoamingVoiceServiceSummaryQueryCreator mssRoamingVoiceServiceSummaryQueryCreator;

    @EJB
    protected MssRoamingSmsAndLocationServiceSummaryQueryCreator mssRoamingSmsAndLocationServiceSummaryQueryCreator;

    @EJB
    protected MssRoamingImsubSummaryQueryCreator mssRoamingImsubSummaryQueryCreator;

    public List<String> constructQuery(final Map<String, Object> templateParameters) {
        return getQuery(templateParameters);
    }

    private List<String> getQuery(final Map<String, Object> templateParameters) {
        final List<String> listOfQueries = new ArrayList<String>();
        listOfQueries.add(mssRoamingVoiceServiceSummaryQueryCreator.createQuery(templateParameters));
        mssRoamingSmsAndLocationServiceSummaryQueryCreator.setToCreateSmsQuery(true);
        listOfQueries.add(mssRoamingSmsAndLocationServiceSummaryQueryCreator.createQuery(templateParameters));
        mssRoamingSmsAndLocationServiceSummaryQueryCreator.setToCreateSmsQuery(false);
        listOfQueries.add(mssRoamingSmsAndLocationServiceSummaryQueryCreator.createQuery(templateParameters));
        listOfQueries.add(mssRoamingImsubSummaryQueryCreator.createQuery(templateParameters));
        return listOfQueries;
    }

    /**
     * @param mssRoamingVoiceServiceSummaryQueryCreator the mssRoamingVoiceServiceSummaryQueryCreator to set
     */
    public void setMssRoamingVoiceServiceSummaryQueryCreator(
            final MssRoamingVoiceServiceSummaryQueryCreator mssRoamingVoiceServiceSummaryQueryCreator) {
        this.mssRoamingVoiceServiceSummaryQueryCreator = mssRoamingVoiceServiceSummaryQueryCreator;
    }

    /**
     * @param mssRoamingSmsAndLocationServiceSummaryQueryCreator the mssRoamingSmsAndLocationServiceSummaryQueryCreator to set
     */
    public void setMssRoamingSmsAndLocationServiceSummaryQueryCreator(
            final MssRoamingSmsAndLocationServiceSummaryQueryCreator mssRoamingSmsAndLocationServiceSummaryQueryCreator) {
        this.mssRoamingSmsAndLocationServiceSummaryQueryCreator = mssRoamingSmsAndLocationServiceSummaryQueryCreator;
    }

    /**
     * @param mssRoamingImsubSummaryQueryCreator the mssRoamingImsubSummaryQueryCreator to set
     */
    public void setMssRoamingImsubSummaryQueryCreator(
            final MssRoamingImsubSummaryQueryCreator mssRoamingImsubSummaryQueryCreator) {
        this.mssRoamingImsubSummaryQueryCreator = mssRoamingImsubSummaryQueryCreator;
    }

}
