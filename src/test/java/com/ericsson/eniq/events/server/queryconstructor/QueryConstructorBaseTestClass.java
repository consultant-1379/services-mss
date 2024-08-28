package com.ericsson.eniq.events.server.queryconstructor;

import javax.ejb.EJB;

import org.junit.Ignore;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

import com.ericsson.eniq.events.server.queryconstructor.QueryConstructorUtils;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary.MssSummaryTemplateInfoToTypeMappings;
import com.ericsson.eniq.events.server.templates.utils.TemplateUtils;

//specifies the Spring configuration to load for this test fixture
@ContextConfiguration(locations = { "classpath:com/ericsson/eniq/events/server/queryconstructor/QueryConstructorBaseTestClass-context.xml" })
@Ignore
public class QueryConstructorBaseTestClass extends AbstractJUnit4SpringContextTests {

    @EJB
    protected TemplateUtils templateUtils;

    @EJB
    protected MssSummaryTemplateInfoToTypeMappings sumTempInfoToTypeMappings;

    @EJB
    protected QueryConstructorUtils queryConstructorUtils;
}
