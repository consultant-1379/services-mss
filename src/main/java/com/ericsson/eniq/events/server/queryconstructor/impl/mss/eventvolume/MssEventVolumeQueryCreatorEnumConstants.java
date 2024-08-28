package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume;

/**
 * @author ezhibhe
 * @since 2011
 */
public class MssEventVolumeQueryCreatorEnumConstants {

    /**
     * RawTableType is used for enum of SUC, ERR and DROP_CALL (Voice), but SMS or LOC_SERVICE has only SUC or ERR
     */
    static enum RawTableType {
        SUC, ERR, DROP_CALL
    }

    /**
     * MssTableType is used for enum of Voice, SMS and loc_service
     */
    static enum MssTableType {
        VOICE, SMS, LOC_SERVICE
    }

}
