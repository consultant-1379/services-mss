/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss.piechart;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Utility class that holds the constants used in #MSSCauseCodeAnalysisPieChartResource
 * 
 * @author eavidat
 * @since 2011
 *
 */
public class MSSCauseCodeAnalysisPieChartConstants {

    static final String CAUSE_CODE_IDS = "causeCodeIds";

    static final String IS_GROUP = "isgroup";

    static final String GROUP_TABLE_NAME = "groupTable";

    static final String GROUP_COLUMN_NAME = "groupnameColumn";

    static final String DROP_CALL_MSS_TABLES = "dropCallMssTables";

    static final String templateParameterEventsQuery = "QUERY_PART_EVENTS";

    static final String templateParameterSubscribersQuery = "QUERY_PART_SUBSCRIBERS";

    static final Map<String, List<String>> mssColumns = new HashMap<String, List<String>>();

    static final Map<String, String> mssGroupJoinKey = new HashMap<String, String>();

    static {
        final List<String> mssMscColumns = new ArrayList<String>();
        mssMscColumns.add(EVENT_SOURCE_SQL_ID);
        mssColumns.put(TYPE_MSC, mssMscColumns);
        final List<String> mssBscColumns = new ArrayList<String>();
        mssBscColumns.add(CONTROLLER_SQL_ID);
        mssColumns.put(TYPE_BSC, mssBscColumns);
        final List<String> mssCellColumns = new ArrayList<String>();
        mssCellColumns.add(CELL_SQL_ID);
        mssColumns.put(TYPE_CELL, mssCellColumns);

        mssGroupJoinKey.put(TYPE_MSC, EVENT_SOURCE_SQL_ID);
        mssGroupJoinKey.put(TYPE_BSC, CONTROLLER_SQL_ID);
        mssGroupJoinKey.put(TYPE_CELL, CELL_SQL_ID);
    }
}