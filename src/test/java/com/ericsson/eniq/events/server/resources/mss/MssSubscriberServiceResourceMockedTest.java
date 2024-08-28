package com.ericsson.eniq.events.server.resources.mss;

import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;

/**
 * 
 * @author echimma
 * @since 2011
 *
 */
public class MssSubscriberServiceResourceMockedTest extends BaseJMockUnitTest {
    private MssSubscriberServiceResource mssSubscriberServiceResource;

    @Before
    public void setUp() {

        mssSubscriberServiceResource = new MssSubscriberServiceResource();

    }

    @Test
    public void testGetMssSubsessionBIResource() {
        final MssSubsessionBIResource mockMssSubsessionBIResource = mockery.mock(MssSubsessionBIResource.class);
        mssSubscriberServiceResource.mssSubsessionBIResource = mockMssSubsessionBIResource;
        assertNotNull(mssSubscriberServiceResource.getSubsessionBIResource());
    }
}
