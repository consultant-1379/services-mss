/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.util.ArrayList;
import java.util.List;

public enum MssSummaryQueryCreatorColumnInfo {

    COMMON_COLUMNS(new String[]{"EVENT_ID"}),

    TAC_SELECT_COLUMNS(new String[]{"TAC"}),
    MANUFAC_SELECT_COLUMNS(new String[]{"MANUFACTURER"}),
    MSC_SELECT_COLUMNS(new String[]{"EVENT_SOURCE_NAME",EVENT_SOURCE_SQL_ID}),
    CONTROLLER_HIER321_SELECT_COLUMNS(new String[]{"VENDOR","HIERARCHY_3",CONTROLLER_SQL_ID}),
    CELL_HIER321_SELECT_COLUMNS(new String[]{"VENDOR","HIERARCHY_3","HIERARCHY_1",CELL_SQL_ID}),
    
    TAC_TACGROUP_JOIN_COLUMNS(new String[]{"TAC"}),
    MANUFAC_JOIN_COLUMNS(new String[]{"MANUFACTURER"}),
    MSC_MSCGROUP_JOIN_COLUMNS(new String[]{EVENT_SOURCE_SQL_ID}),
    CONTROLLER_CONTROLLERGROUP_HIER321_JOIN_COLUMNS(new String[]{CONTROLLER_SQL_ID}),
    CELL_CELLGROUP_HIER321_JOIN_COLUMNS(new String[]{CELL_SQL_ID}),
    IMSI_JOIN_COLUMN(new String[]{"IMSI"}),
    MSISDN_JOIN_COLUMN(new String[]{"MSISDN"}),

    TAC_TACGROUP_MANUFAC_FILTER_COLUMNS(new String[]{"TAC","EVENT_ID"}),
    MSC_MSCGROUP_FILTER_COLUMNS(new String[]{EVENT_SOURCE_SQL_ID,"EVENT_ID"}),
    CONTROLLER_CONTROLLERGROUP_FILTER_COLUMNS(new String[]{CONTROLLER_SQL_ID,"EVENT_ID"}),
    CELL_CELLGROUP_FILTER_COLUMNS(new String[]{CELL_SQL_ID,"EVENT_ID"}),
    IMSI_GROUP_FILTER_COLUMNS(new String[]{"IMSI","EVENT_ID"}),
    MSISDN_GROUP_FILTER_COLUMNS(new String[]{"MSISDN","EVENT_ID"}),
    
    MANUF_EXTRA_TABLE_JOIN_COLUMN(new String[]{"TAC"}),
    
    SUCCESS_ERROR_COLUMN_RAW(new String[]{"count(*)"}),
    ERROR_COLUMN_AGGREGATION(new String[]{"NO_OF_ERRORS"}),
    SUCCESS_COLUMN_AGGREGATION(new String[]{"NO_OF_SUCCESSES"}),
    DROPPED_COLUMN_ALIAS(new String[]{"NO_OF_DROPPED_CALLS"}),
    BLOCKED_COLUMN_ALIAS(new String[]{"NO_OF_BLOCKED_CALLS"}),
    SUCCESS_COLUMN_ALIAS(new String[]{"NO_OF_SUCCESSES"}),
    ;

    private MssSummaryQueryCreatorColumnInfo(final String[] columns){
        columnInfo = columns;
    }

    private final String[] columnInfo ;

    public List<String> getColumnInfo() {
        final List<String> listOfColumns =  new ArrayList<String>();
        for(final String column : columnInfo){
            listOfColumns.add(column);
        }
        return listOfColumns;
    }
}
