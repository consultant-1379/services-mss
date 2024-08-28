/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss;

import org.junit.Ignore;

import com.ericsson.eniq.events.server.queryconstructor.QueryConstructor;
import com.ericsson.eniq.events.server.queryconstructor.QueryConstructorUtils;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.MssEventAnalysisQueryHelper;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.errSucTotal.MssEventAnalysisErrSucTotalQueryCreator;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary.MssSmsAndLocationServiceSummaryQueryCreator;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary.MssSummaryTemplateInfoToTypeMappings;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary.MssVoiceServiceSummaryQueryCreator;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.subbi.MssSubscriberBIQueryHelper;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.subbi.summary.MssSubscriberBISmsAndLocServiceSummaryQueryCreator;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.subbi.summary.MssSubscriberBIVoiceSummaryQueryCreator;
import com.ericsson.eniq.events.server.resources.DataServiceBaseTestCase;
import com.ericsson.eniq.events.server.services.impl.TechPackCXCMappingService;

/**
 * @author eemecoy
 */
@Ignore
public class MssDataServiceBaseTestCase extends DataServiceBaseTestCase {

    protected MssSummaryTemplateInfoToTypeMappings sumTempInfoToTypeMappings;

    protected QueryConstructorUtils queryConstructorUtils;

    protected QueryConstructor queryConstructor;

    protected MssEventAnalysisQueryHelper mssEvntAnalysisQueryHelper;

    protected MssVoiceServiceSummaryQueryCreator mssVoiceServiceSummaryQueryCreator;

    protected MssEventAnalysisErrSucTotalQueryCreator mssEventAnalysisErrSucTotalQueryCreator;

    protected MssSubscriberBIQueryHelper mssSubscriberBIQueryHelper;

    protected MssSubscriberBIVoiceSummaryQueryCreator mssSubscriberBIVoiceSummaryQueryCreator;

    protected MssSubscriberBISmsAndLocServiceSummaryQueryCreator mssSubscriberBISmsAndLocServiceSummaryQueryCreator;

    protected MssSmsAndLocationServiceSummaryQueryCreator mssSmsAndLocationServiceSummaryQueryCreator;

    protected TechPackCXCMappingService techPackCXCMappingService;

    protected void attachDependenciesForMssBaseResource(final MssBaseResource mssBaseResource) {
        attachDependencies(mssBaseResource);
        mssBaseResource.setRMIEngineUtils(rmiEngineUtils);
        mssBaseResource.setTechPackCXCMappingService(techPackCXCMappingService);
    }

    public MssSummaryTemplateInfoToTypeMappings getSumTempInfoToTypeMappings() {
        return sumTempInfoToTypeMappings;
    }

    public void setSumTempInfoToTypeMappings(final MssSummaryTemplateInfoToTypeMappings sumTempInfoToTypeMappings) {
        this.sumTempInfoToTypeMappings = sumTempInfoToTypeMappings;
    }

    public QueryConstructorUtils getQueryConstructorUtils() {
        return queryConstructorUtils;
    }

    public void setQueryConstructorUtils(final QueryConstructorUtils queryConstructorUtils) {
        this.queryConstructorUtils = queryConstructorUtils;
    }

    public QueryConstructor getQueryConstructor() {
        return queryConstructor;
    }

    public void setQueryConstructor(final QueryConstructor queryConstructor) {
        this.queryConstructor = queryConstructor;
    }

    public MssEventAnalysisQueryHelper getMssEvntAnalysisQueryHelper() {
        return mssEvntAnalysisQueryHelper;
    }

    public void setMssEvntAnalysisQueryHelper(final MssEventAnalysisQueryHelper mssEvntAnalysisQueryHelper) {
        this.mssEvntAnalysisQueryHelper = mssEvntAnalysisQueryHelper;
    }

    public MssVoiceServiceSummaryQueryCreator getMssVoiceServiceSummaryQueryCreator() {
        return mssVoiceServiceSummaryQueryCreator;
    }

    public void setMssVoiceServiceSummaryQueryCreator(
            final MssVoiceServiceSummaryQueryCreator mssVoiceServiceSummaryQueryCreator) {
        this.mssVoiceServiceSummaryQueryCreator = mssVoiceServiceSummaryQueryCreator;
    }

    public MssEventAnalysisErrSucTotalQueryCreator getMssEventAnalysisErrSucTotalQueryCreator() {
        return mssEventAnalysisErrSucTotalQueryCreator;
    }

    public void setMssEventAnalysisErrSucTotalQueryCreator(
            final MssEventAnalysisErrSucTotalQueryCreator mssEventAnalysisErrSucTotalQueryCreator) {
        this.mssEventAnalysisErrSucTotalQueryCreator = mssEventAnalysisErrSucTotalQueryCreator;
    }

    public MssSubscriberBIQueryHelper getMssSubscriberBIQueryHelper() {
        return mssSubscriberBIQueryHelper;
    }

    public void setMssSubscriberBIQueryHelper(final MssSubscriberBIQueryHelper mssSubscriberBIQueryHelper) {
        this.mssSubscriberBIQueryHelper = mssSubscriberBIQueryHelper;
    }

    public MssSubscriberBIVoiceSummaryQueryCreator getMssSubscriberBIVoiceSummaryQueryCreator() {
        return mssSubscriberBIVoiceSummaryQueryCreator;
    }

    public void setMssSubscriberBIVoiceSummaryQueryCreator(
            final MssSubscriberBIVoiceSummaryQueryCreator mssSubscriberBIVoiceSummaryQueryCreator) {
        this.mssSubscriberBIVoiceSummaryQueryCreator = mssSubscriberBIVoiceSummaryQueryCreator;
    }

    public MssSubscriberBISmsAndLocServiceSummaryQueryCreator getMssSubscriberBISmsAndLocServiceSummaryQueryCreator() {
        return mssSubscriberBISmsAndLocServiceSummaryQueryCreator;
    }

    public void setTechPackCXCMappingService(final TechPackCXCMappingService techPackCXCMappingService) {
        this.techPackCXCMappingService = techPackCXCMappingService;
    }

    public void setMssSubscriberBISmsAndLocServiceSummaryQueryCreator(
            final MssSubscriberBISmsAndLocServiceSummaryQueryCreator mssSubscriberBISmsAndLocServiceSummaryQueryCreator) {
        this.mssSubscriberBISmsAndLocServiceSummaryQueryCreator = mssSubscriberBISmsAndLocServiceSummaryQueryCreator;
    }

    public MssSmsAndLocationServiceSummaryQueryCreator getMssSmsAndLocationServiceSummaryQueryCreator() {
        return mssSmsAndLocationServiceSummaryQueryCreator;
    }

    public void setMssSmsAndLocationServiceSummaryQueryCreator(
            final MssSmsAndLocationServiceSummaryQueryCreator mssSmsAndLocationServiceSummaryQueryCreator) {
        this.mssSmsAndLocationServiceSummaryQueryCreator = mssSmsAndLocationServiceSummaryQueryCreator;
    }

    @Override
    protected String[] getConfigLocations() {
        return new String[] { "classpath:com/ericsson/eniq/events/server/resources/MssDataServiceBaseTestCase-context.xml" };
    }

}
