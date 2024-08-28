package com.ericsson.eniq.events.server.resources.mss;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.GroupMgtResource;
import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;

/**
 * 
 * @author echimma
 * @since 2011
 *
 */
public class MssTerminalServiceResourceMockedTest extends BaseJMockUnitTest {
    private MssTerminalServiceResource mssTerminalServiceResource;

    @Before
    public void setUp() {

        mssTerminalServiceResource = new MssTerminalServiceResource();

    }

    @Test
    public void testGetMssEventAnalysisResource() {
        final MssEventAnalysisResource mockMssEventAnalysisResource = mockery.mock(MssEventAnalysisResource.class);
        mssTerminalServiceResource.mssEventAnalysisResource = mockMssEventAnalysisResource;
        assertNotNull(mssTerminalServiceResource.getMssEventAnalysisResource());
    }

    @Test
    public void testGetMssMultipleRankingResource() {
        final MssMultipleRankingResource mockMssMultipleRankingResource = mockery
                .mock(MssMultipleRankingResource.class);
        mssTerminalServiceResource.mssMultipleRankingResource = mockMssMultipleRankingResource;
        assertNotNull(mssTerminalServiceResource.getMssMultipleRankingResource());
    }

    @Test
    public void testGetMssKPIResource() {
        final MssKPIResource mockMssKpiResource = mockery.mock(MssKPIResource.class);
        mssTerminalServiceResource.mssKPIResource = mockMssKpiResource;
        assertNotNull(mssTerminalServiceResource.getMssKpiResource());
    }

    @Test
    public void testGetGroupMgtResource() {
        final GroupMgtResource mockGroupMgtResource = mockery.mock(GroupMgtResource.class);
        mssTerminalServiceResource.groupMgtResource = mockGroupMgtResource;
        final String groupName = "aGroup";
        final String someData = "some data";
        mockery.checking(new Expectations() {
            {
                one(mockGroupMgtResource).getGroupDetails(groupName);
                will(returnValue(someData));
            }
        });
        assertNotNull(mssTerminalServiceResource.getGroupMgtResource());
        assertEquals(someData, mssTerminalServiceResource.getGroupMgtResource().getGroupDetails(groupName));
    }
}
