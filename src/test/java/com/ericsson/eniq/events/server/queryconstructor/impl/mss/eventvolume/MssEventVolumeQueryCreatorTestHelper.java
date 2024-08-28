package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ericsson.eniq.events.server.common.Group;
import com.ericsson.eniq.events.server.common.tablesandviews.TableType;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPack;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryCreatorEnumConstants.MssTableType;

public abstract class MssEventVolumeQueryCreatorTestHelper {

    private static Map<MssTableType, Boolean> mssTableTypes = new HashMap<MssTableType, Boolean>();

    static {
        mssTableTypes.put(MssTableType.VOICE, true);
        mssTableTypes.put(MssTableType.SMS, true);
        mssTableTypes.put(MssTableType.LOC_SERVICE, true);
    }

    public static Map<MssTableType, Boolean> getMssTableTypes() {
        return mssTableTypes;
    }

    public static TechPackTables populateTables(final String type, final String timerange) {
        if (RAW.equals(timerange)) {
            return populateRawTables();
        }
        String tableTypeVoice = "EVNTSRC";
        String tableTypeLocService = "EVNTSRC";
        String tableTypeSMS = "EVNTSRC";
        if (NO_TYPE.equals(type)) {
            tableTypeVoice = "EVNTSRC_EVENTID";
            tableTypeLocService = "EVNTSRC";
            tableTypeSMS = "EVNTSRC";
        }
        if (TYPE_MSC.equals(type)) {
            tableTypeVoice = "EVNTSRC_EVENTID";
            tableTypeLocService = "EVNTSRC";
            tableTypeSMS = "EVNTSRC";
        } else if (TYPE_BSC.equals(type)) {
            tableTypeVoice = "VEND_HIER3_EVENTID";
            tableTypeLocService = "VEND_HIER321";
            tableTypeSMS = "VEND_HIER321";
        } else if (TYPE_CELL.equals(type)) {
            tableTypeVoice = "VEND_HIER321_EVENTID";
            tableTypeLocService = "VEND_HIER321";
            tableTypeSMS = "VEND_HIER321";
        }
        final String time = (FIFTEEN_MINUTES.equals(timerange) ? "15MIN" : "DAY");
        final TechPackTables techPackTables = new TechPackTables(TableType.RAW);
        final TechPack techPack = new TechPack("EVENT_E_MSS", TableType.RAW, "DIM_E_MSS");
        techPackTables.addTechPack(techPack);

        // VOICE
        final List<String> sucTablesRaw = new ArrayList<String>();
        sucTablesRaw.add(String.format("EVENT_E_MSS_VOICE_%s_SUC_%s", tableTypeVoice, time));
        final List<String> errTablesRawDropped = new ArrayList<String>();
        errTablesRawDropped.add(String.format("EVENT_E_MSS_VOICE_%s_DROP_CALL_%s", tableTypeVoice, time));
        final List<String> errTablesRawBlocked = new ArrayList<String>();
        errTablesRawBlocked.add(String.format("EVENT_E_MSS_VOICE_%s_ERR_%s", tableTypeVoice, time));
        techPack.setErrRawTables(KEY_TYPE_ERR, errTablesRawBlocked);
        techPack.setErrRawTables(KEY_TYPE_DROP_CALL, errTablesRawDropped);
        techPack.setSucRawTables(sucTablesRaw);

        // LOC_SERVICE
        final List<String> sucTablesRawLocService = new ArrayList<String>();
        sucTablesRawLocService.add(String.format("EVENT_E_MSS_LOC_SERVICE_%s_SUC_%s", tableTypeLocService, time));
        final List<String> errTablesRawLocService = new ArrayList<String>();
        errTablesRawLocService.add(String.format("EVENT_E_MSS_LOC_SERVICE_%s_ERR_%s", tableTypeLocService, time));
        techPack.setErrRawTables(KEY_TYPE_LOC_SERVICE_ERR, errTablesRawLocService);
        techPack.setSucRawTables(KEY_TYPE_LOC_SERVICE_SUC, sucTablesRawLocService);
        
        // LOC_SERVICE
        final List<String> sucTablesRawSMS = new ArrayList<String>();
        sucTablesRawLocService.add(String.format("EVENT_E_MSS_SMS_%s_SUC_%s", tableTypeSMS, time));
        final List<String> errTablesRawSMS = new ArrayList<String>();
        errTablesRawLocService.add(String.format("EVENT_E_MSS_SMS_%s_ERR_%s", tableTypeSMS, time));
        techPack.setErrRawTables(KEY_TYPE_SMS_ERR, sucTablesRawSMS);
        techPack.setSucRawTables(KEY_TYPE_SMS_SUC, errTablesRawSMS);

        return techPackTables;
    }

    public static TechPackTables populateRawTables() {
        final TechPackTables techPackTables = new TechPackTables(TableType.RAW);
        final TechPack techPack = new TechPack("EVENT_E_MSS", TableType.RAW, "DIM_E_MSS");
        techPackTables.addTechPack(techPack);

        // VOICE
        final List<String> sucTablesRawVoice = new ArrayList<String>();
        sucTablesRawVoice.add(EVENT_E_MSS_VOICE_CDR_SUC_RAW);
        final List<String> errTablesRawDroppedVoice = new ArrayList<String>();
        errTablesRawDroppedVoice.add(EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW);
        final List<String> errTablesRawBlockedVoice = new ArrayList<String>();
        errTablesRawBlockedVoice.add(EVENT_E_MSS_VOICE_CDR_ERR_RAW);
        techPack.setErrRawTables(KEY_TYPE_ERR, errTablesRawBlockedVoice);
        techPack.setErrRawTables(KEY_TYPE_DROP_CALL, errTablesRawDroppedVoice);
        techPack.setSucRawTables(sucTablesRawVoice);

        // LOC_SERVICE
        final List<String> sucTablesRawLocService = new ArrayList<String>();
        sucTablesRawLocService.add(EVENT_E_MSS_LOC_SERVICE_CDR_SUC_RAW);
        final List<String> errTablesRawLocService = new ArrayList<String>();
        errTablesRawLocService.add(EVENT_E_MSS_LOC_SERVICE_CDR_ERR_RAW);
        techPack.setErrRawTables(KEY_TYPE_LOC_SERVICE_ERR, errTablesRawLocService);
        techPack.setSucRawTables(KEY_TYPE_LOC_SERVICE_SUC, sucTablesRawLocService);
        
        //SMS
        final List<String> sucTablesRawSMS = new ArrayList<String>();
        sucTablesRawLocService.add(EVENT_E_MSS_SMS_CDR_SUC_RAW);
        final List<String> errTablesRawSMS = new ArrayList<String>();
        errTablesRawLocService.add(EVENT_E_MSS_SMS_CDR_ERR_RAW);
        techPack.setErrRawTables(KEY_TYPE_SMS_ERR, sucTablesRawSMS);
        techPack.setSucRawTables(KEY_TYPE_SMS_SUC, errTablesRawSMS);
        
        return techPackTables;
    }

    public static Map<String, Group> populateGroupDefintion() {
        final Map<String, Group> groupDefinitions = new HashMap<String, Group>();
        final Group mssGroup = new Group(TYPE_MSC, "GROUP_TYPE_E_EVNTSRC_CS", null, new ArrayList<String>());
        final Group bscGroup = new Group(TYPE_BSC, "GROUP_TYPE_E_RAT_VEND_HIER3", null, new ArrayList<String>());
        final Group cellGroup = new Group(TYPE_CELL, "GROUP_TYPE_E_RAT_VEND_HIER321", null, new ArrayList<String>());
        groupDefinitions.put(GROUP_TYPE_EVENT_SRC_CS, mssGroup);
        groupDefinitions.put(GROUP_TYPE_HIER3, bscGroup);
        groupDefinitions.put(GROUP_TYPE_HIER1, cellGroup);
        return groupDefinitions;
    }

    public static int getInterval(final String timerange) {
        if (DAY.equals(timerange)) {
            return 1440;
        } else if (FIFTEEN_MINUTES.equals(timerange)) {
            return 15;
        } else {
            return 1;
        }
    }

}
