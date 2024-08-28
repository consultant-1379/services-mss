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
public class MssServiceResourceMockedTest extends BaseJMockUnitTest {
    private MssServiceResource mssServiceResource;

    @Before
    public void setUp() {

        mssServiceResource = new MssServiceResource();

    }

    @Test
    public void testGetMssNetworkServiceResource() {
        final MssNetworkServiceResource mockMssNetworkServiceResource = mockery.mock(MssNetworkServiceResource.class);
        mssServiceResource.mssNetworkServiceResource = mockMssNetworkServiceResource;
        assertNotNull(mssServiceResource.getMssNetworkServiceResource());
    }

    @Test
    public void testGetMssTerminalServiceResource() {
        final MssTerminalServiceResource mockMssTerminalServiceResource = mockery
                .mock(MssTerminalServiceResource.class);
        mssServiceResource.mssTerminalServiceResource = mockMssTerminalServiceResource;
        assertNotNull(mssServiceResource.getMssTerminalServiceResource());
    }

    @Test
    public void testGetMssSubscriberServiceResource() {
        final MssSubscriberServiceResource mockMssSubscriberServiceResource = mockery
                .mock(MssSubscriberServiceResource.class);
        mssServiceResource.mssSubscriberServiceResource = mockMssSubscriberServiceResource;
        assertNotNull(mssServiceResource.getMssSubscriberServiceResource());
    }
}
