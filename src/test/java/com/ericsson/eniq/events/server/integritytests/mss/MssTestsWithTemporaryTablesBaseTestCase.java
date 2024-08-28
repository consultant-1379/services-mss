package com.ericsson.eniq.events.server.integritytests.mss;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.resources.mss.MssBaseResource;
import com.ericsson.eniq.events.server.services.impl.TechPackCXCMappingService;
import com.ericsson.eniq.events.server.templates.utils.TemplateUtils;
import com.ericsson.eniq.events.server.test.queryresults.QueryResult;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.schema.ColumnDetails;
import com.ericsson.eniq.events.server.test.schema.MssColumnTypes;
import com.ericsson.eniq.events.server.test.schema.Nullable;
import com.ericsson.eniq.events.server.utils.RMIEngineUtils;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:com/ericsson/eniq/events/server/resources/MssTestsWithTemporaryTablesBaseTestCase-context.xml" })
public abstract class MssTestsWithTemporaryTablesBaseTestCase<T extends QueryResult> extends
        TestsWithTemporaryTablesBaseTestCase<T> {

    protected TemplateUtils templateUtils;

    protected MssSummaryTemplateInfoToTypeMappings sumTempInfoToTypeMappings;

    protected QueryConstructorUtils queryConstructorUtils;

    @Autowired
    protected QueryConstructor queryConstructor;

    protected MssEventAnalysisQueryHelper mssEvntAnalysisQueryHelper;

    protected MssVoiceServiceSummaryQueryCreator mssVoiceServiceSummaryQueryCreator;

    protected MssEventAnalysisErrSucTotalQueryCreator mssEventAnalysisErrSucTotalQueryCreator;

    protected MssSubscriberBIQueryHelper mssSubscriberBIQueryHelper;

    protected MssSubscriberBIVoiceSummaryQueryCreator mssSubscriberBIVoiceSummaryQueryCreator;

    protected MssSubscriberBISmsAndLocServiceSummaryQueryCreator mssSubscriberBISmsAndLocServiceSummaryQueryCreator;

    protected MssSmsAndLocationServiceSummaryQueryCreator mssSmsAndLocationServiceSummaryQueryCreator;

    // protected TechPackCXCMappingService techPackCXCMappingService;

    @Autowired
    protected TechPackCXCMappingService techPackCXCMappingService;

    @Autowired
    protected RMIEngineUtils rmiEngineUtils;

    protected void attachDependenciesForMSSBaseResource(final MssBaseResource mssBaseResource) {
        attachDependencies(mssBaseResource);
        mssBaseResource.setRMIEngineUtils(rmiEngineUtils);
        mssBaseResource.setTechPackCXCMappingService(techPackCXCMappingService);
    }

    protected ResultTranslator<T> getTranslator(final T t) {
        if (resultTranslator == null) {
            resultTranslator = new ResultTranslator<T>();
        }
        return resultTranslator;
    }

    public QueryConstructor getQueryConstructor() {
        return queryConstructor;
    }

    public void setQueryConstructor(final QueryConstructor queryConstructor) {
        this.queryConstructor = queryConstructor;
    }

    public void setSumTempInfoToTypeMappings(final MssSummaryTemplateInfoToTypeMappings sumTempInfoToTypeMappings) {
        this.sumTempInfoToTypeMappings = sumTempInfoToTypeMappings;
    }

    public void setQueryConstructorUtils(final QueryConstructorUtils queryConstructorUtils) {
        this.queryConstructorUtils = queryConstructorUtils;
    }

    public void setMssEvntAnalysisQueryHelper(final MssEventAnalysisQueryHelper mssEvntAnalysisQueryHelper) {
        this.mssEvntAnalysisQueryHelper = mssEvntAnalysisQueryHelper;
    }

    public void setMssVoiceServiceSummaryQueryCreator(
            final MssVoiceServiceSummaryQueryCreator mssVoiceServiceSummaryQueryCreator) {
        this.mssVoiceServiceSummaryQueryCreator = mssVoiceServiceSummaryQueryCreator;
    }

    public void setMssEventAnalysisErrSucTotalQueryCreator(
            final MssEventAnalysisErrSucTotalQueryCreator mssEventAnalysisErrSucTotalQueryCreator) {
        this.mssEventAnalysisErrSucTotalQueryCreator = mssEventAnalysisErrSucTotalQueryCreator;
    }

    public void setMssSubscriberBIQueryHelper(final MssSubscriberBIQueryHelper mssSubscriberBIQueryHelper) {
        this.mssSubscriberBIQueryHelper = mssSubscriberBIQueryHelper;
    }

    public void setMssSubscriberBIVoiceSummaryQueryCreator(
            final MssSubscriberBIVoiceSummaryQueryCreator mssSubscriberBIVoiceSummaryQueryCreator) {
        this.mssSubscriberBIVoiceSummaryQueryCreator = mssSubscriberBIVoiceSummaryQueryCreator;
    }

    public void setMssSubscriberBISmsAndLocServiceSummaryQueryCreator(
            final MssSubscriberBISmsAndLocServiceSummaryQueryCreator mssSubscriberBISmsAndLocServiceSummaryQueryCreator) {
        this.mssSubscriberBISmsAndLocServiceSummaryQueryCreator = mssSubscriberBISmsAndLocServiceSummaryQueryCreator;
    }

    public void setMssSmsAndLocationServiceSummaryQueryCreator(
            final MssSmsAndLocationServiceSummaryQueryCreator mssSmsAndLocationServiceSummaryQueryCreator) {
        this.mssSmsAndLocationServiceSummaryQueryCreator = mssSmsAndLocationServiceSummaryQueryCreator;
    }

    public void setTechPackCXCMappingService(final TechPackCXCMappingService techPackCXCMappingService) {
        this.techPackCXCMappingService = techPackCXCMappingService;
    }

    @SuppressWarnings("unchecked")
    public void createTemporaryTableMss(final String table, final Collection<String> columns) throws Exception {
        final Map<String, String> columnsWithTypes = getColumnTypesMss(columns);
        final List<ColumnDetails> columnsWithTypesAndNullableInfo = addDefaultNullableInfo(columnsWithTypes);
        createTemporaryTable(table, columnsWithTypesAndNullableInfo);
    }

    public void createTemporaryTableOnSpecificConnectionMss(final Connection conn, final String table,
            final Collection<String> columns) throws Exception {
        final Map<String, String> columnsWithTypes = getColumnTypesMss(columns);
        createTemporaryTableOnSpecificConnection(conn, table, columnsWithTypes);
    }

    private List<ColumnDetails> addDefaultNullableInfo(final Map<String, String> columnsWithTypes) {
        final List<ColumnDetails> columnDetails = new ArrayList<ColumnDetails>();
        for (final String columnName : columnsWithTypes.keySet()) {
            columnDetails.add(new ColumnDetails(columnName, columnsWithTypes.get(columnName), Nullable.CANNOT_BE_NULL));
        }
        return columnDetails;
    }

    private Map<String, String> getColumnTypesMss(final Collection<String> columns) {
        final Map<String, String> columnTypes = new HashMap<String, String>();
        for (final String column : columns) {
            columnTypes.put(column, MssColumnTypes.getColumnType(column));
        }
        return columnTypes;
    }
}
