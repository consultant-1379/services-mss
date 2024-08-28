package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume;

import org.junit.Test;

import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryCreatorEnumConstants.RawTableType;

public class MssEventVolumeQueryCreatorEnumConstantsTest {

    @Test
    public void testRawTableType() {
        // Only for coverage
        RawTableType.values();
        RawTableType.valueOf("SUC");
    }

}
