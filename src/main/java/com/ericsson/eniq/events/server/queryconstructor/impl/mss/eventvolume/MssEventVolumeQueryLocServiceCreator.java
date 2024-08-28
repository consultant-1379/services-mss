package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.QueryConstructorConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryConstants.*;

import javax.ejb.Stateless;

import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryCreatorEnumConstants.MssTableType;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryCreatorEnumConstants.RawTableType;

@Stateless
public class MssEventVolumeQueryLocServiceCreator extends MssEventVolumeQueryBaseCreator {

    @Override
    protected String getQuery() {
        if (!mssTableTypes.containsKey(MssTableType.LOC_SERVICE)
                || !mssTableTypes.get(MssTableType.LOC_SERVICE).booleanValue()) {
            return "";
        }
        final String dataSubsql = createDataTableQuery();
        eventVolumeParameters.put(DATA_TABLE_SUBSQL_VAR, dataSubsql);
        return templateUtils.getQueryFromTemplate(MSS_EVENT_VOLUME_LOC_SERVICE_TEMPLATE, eventVolumeParameters);
    }

    /**
     * createDataTableQuery is used to get data table from blocked, dropped call and success
     * @return data query
     */
    private String createDataTableQuery() {
        final String blockedSubsql = createOriginalTable(MssTableType.LOC_SERVICE, RawTableType.ERR,
                COMMON_COLUMNS_LOC_SERVICE, NO_OF_BLOCKED_CALLS, BLOCKED_TABLE_NAME);
        final String sucSubsql = createOriginalTable(MssTableType.LOC_SERVICE, RawTableType.SUC,
                COMMON_COLUMNS_LOC_SERVICE, NO_OF_SUCCESSES_CALLS, SUC_TABLE_NAME);
        eventVolumeParameters.put(COMMON_COLUMNS_VAR, COMMON_COLUMNS_LOC_SERVICE);
        eventVolumeParameters.put(LEFT_TABLE_VAR, BLOCKED_TABLE_NAME);
        eventVolumeParameters.put(RIGHT_TABLE_VAR, SUC_TABLE_NAME);
        eventVolumeParameters.put(LEFT_TABLE_SUBSQL_VAR, blockedSubsql);
        eventVolumeParameters.put(RIGHT_TABLE_SUBSQL_VAR, sucSubsql);
        diffColumns.add(NO_OF_BLOCKED_CALLS);
        diffColumns.add(NO_OF_SUCCESSES_CALLS);
        eventVolumeParameters.put(DIFF_COLUMNS_VAR, diffColumns);
        eventVolumeParameters.put(TABLES_ALIAS_VAR, DATA_TABLE_NAME);
        return templateUtils.getQueryFromTemplate(MSS_FULL_OUTER_JOIN_TEMPLATE, eventVolumeParameters);
    }

}
