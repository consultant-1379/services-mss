package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.QueryConstructorConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryConstants.*;

import java.util.ArrayList;
import java.util.List;

import javax.ejb.Stateless;

import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryCreatorEnumConstants.MssTableType;
import com.ericsson.eniq.events.server.utils.Pair;

@Stateless
public class MssEventVolumeQueryImsubCreator extends MssEventVolumeQueryBaseCreator {

    @Override
    protected String getQuery() {
        if (eventVolumeParameters.containsKey(TYPE_PARAM) && !NO_TYPE.equals(eventVolumeParameters.get(TYPE_PARAM))) {
            return createImsubTableQuery();
        }
        return "";
    }

    /**
     * createImsubTableQuery is used to get impact subscriber  
     * @return impact subscriber query
     */
    private String createImsubTableQuery() {
        final TechPackTables rawTables = (TechPackTables) eventVolumeParameters.get(TECH_PACK_RAW_TABLES);
        final List<String> imsubTables = new ArrayList<String>();
        if (mssTableTypes.containsKey(MssTableType.VOICE) && mssTableTypes.get(MssTableType.VOICE).booleanValue()) {
            imsubTables.addAll(rawTables.getSucTables());
            imsubTables.addAll(rawTables.getErrTables());
            imsubTables.addAll(rawTables.getErrTables(KEY_TYPE_DROP_CALL));
        }
        if (mssTableTypes.containsKey(MssTableType.LOC_SERVICE)
                && mssTableTypes.get(MssTableType.LOC_SERVICE).booleanValue()) {
            imsubTables.addAll(rawTables.getSucTables(KEY_TYPE_LOC_SERVICE_SUC));
            imsubTables.addAll(rawTables.getErrTables(KEY_TYPE_LOC_SERVICE_ERR));
        }
        if (mssTableTypes.containsKey(MssTableType.SMS) && mssTableTypes.get(MssTableType.SMS).booleanValue()) {
            imsubTables.addAll(rawTables.getSucTables(KEY_TYPE_SMS_SUC));
            imsubTables.addAll(rawTables.getErrTables(KEY_TYPE_SMS_ERR));
        }
        return createImsubTable(imsubTables, COMMON_COLUMNS_IMSI, ORIGINAL_COLUMNS_IMSI_RAW);
    }

    /**
     * createImsubTable is used to create impact subscriber query
     * @param mssTableType  it is voice, sms or loc_service
     * @param tables        tables use to get impact subscribers
     * @param commonColumns 
     * @param filterColumns
     */
    protected String createImsubTable(final List<String> tables, final List<String> commonColumns,
            final List<String> filterColumns) {
        eventVolumeParameters.put(TABLES_VAR, tables);
        eventVolumeParameters.put(COMMON_COLUMNS_VAR, commonColumns);
        eventVolumeParameters.put(FILTER_COLUMNS_VAR, filterColumns);
        eventVolumeParameters.put(WHERE_CONDITIONS_VAR, "");
        eventVolumeParameters.put(USE_EXCLUSIVE_TAC_VAR, !useAggr);
        final Pair<String, String> grpNodeCondition = getGroupConditionNodeCondition();
        eventVolumeParameters.put(GROUP_CONDITION_VAR, grpNodeCondition.first);
        eventVolumeParameters.put(NODE_CONDITION_VAR, grpNodeCondition.second);
        eventVolumeParameters.put(IMSI_TOTAL_VAR, IMSI_VIEW);
        return templateUtils.getQueryFromTemplate(MSS_EVENT_VOLUME_IMSUB_TEMPLATE, eventVolumeParameters);
    }

}
