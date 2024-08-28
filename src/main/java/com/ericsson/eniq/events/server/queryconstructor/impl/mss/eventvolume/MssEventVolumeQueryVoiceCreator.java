package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.QueryConstructorConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryConstants.*;

import javax.ejb.Stateless;

import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryCreatorEnumConstants.MssTableType;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryCreatorEnumConstants.RawTableType;

@Stateless
public class MssEventVolumeQueryVoiceCreator extends MssEventVolumeQueryBaseCreator {

    @Override
    protected String getQuery() {
        if (!mssTableTypes.containsKey(MssTableType.VOICE) || !mssTableTypes.get(MssTableType.VOICE).booleanValue()) {
            return "";
        }
        final String dataSubsql = createDataTableQuery();
        eventVolumeParameters.put(DATA_TABLE_SUBSQL_VAR, dataSubsql);
        return templateUtils.getQueryFromTemplate(MSS_EVENT_VOLUME_VOICE_TEMPLATE, eventVolumeParameters);
    }

    /**
     * createDataTableQuery is used to get data table from blocked, dropped call and success
     * @return data query
     */
    private String createDataTableQuery() {
        final String errSubSql = createErrTableQuery();
        final String sucSubSql = createOriginalTable(MssTableType.VOICE, RawTableType.SUC, COMMON_COLUMNS_VOICE,
                NO_OF_SUCCESSES_CALLS, SUC_TABLE_NAME);
        eventVolumeParameters.put(COMMON_COLUMNS_VAR, COMMON_COLUMNS_VOICE);
        eventVolumeParameters.put(LEFT_TABLE_VAR, ERR_TABLE_NAME);
        eventVolumeParameters.put(RIGHT_TABLE_VAR, SUC_TABLE_NAME);
        eventVolumeParameters.put(LEFT_TABLE_SUBSQL_VAR, errSubSql);
        eventVolumeParameters.put(RIGHT_TABLE_SUBSQL_VAR, sucSubSql);
        diffColumns.add(NO_OF_SUCCESSES_CALLS);
        eventVolumeParameters.put(DIFF_COLUMNS_VAR, diffColumns);
        eventVolumeParameters.put(TABLES_ALIAS_VAR, DATA_TABLE_NAME);
        return templateUtils.getQueryFromTemplate(MSS_FULL_OUTER_JOIN_TEMPLATE, eventVolumeParameters);
    }

    /**
     * createErrTableQuery is get err table from blocked table and dropped call table
     * @return err table query
     */
    private String createErrTableQuery() {
        final String blockedSubSql = createOriginalTable(MssTableType.VOICE, RawTableType.ERR, COMMON_COLUMNS_VOICE,
                NO_OF_BLOCKED_CALLS, BLOCKED_TABLE_NAME);
        final String droppedSubSql = createOriginalTable(MssTableType.VOICE, RawTableType.DROP_CALL,
                COMMON_COLUMNS_VOICE, NO_OF_DROPPED_CALLS, DROPPED_TABLE_NAME);
        eventVolumeParameters.put(COMMON_COLUMNS_VAR, COMMON_COLUMNS_VOICE);
        eventVolumeParameters.put(LEFT_TABLE_VAR, BLOCKED_TABLE_NAME);
        eventVolumeParameters.put(RIGHT_TABLE_VAR, DROPPED_TABLE_NAME);
        eventVolumeParameters.put(LEFT_TABLE_SUBSQL_VAR, blockedSubSql);
        eventVolumeParameters.put(RIGHT_TABLE_SUBSQL_VAR, droppedSubSql);
        diffColumns.add(NO_OF_BLOCKED_CALLS);
        diffColumns.add(NO_OF_DROPPED_CALLS);
        eventVolumeParameters.put(DIFF_COLUMNS_VAR, diffColumns);
        eventVolumeParameters.put(TABLES_ALIAS_VAR, ERR_TABLE_NAME);
        return templateUtils.getQueryFromTemplate(MSS_FULL_OUTER_JOIN_TEMPLATE, eventVolumeParameters);
    }

}
