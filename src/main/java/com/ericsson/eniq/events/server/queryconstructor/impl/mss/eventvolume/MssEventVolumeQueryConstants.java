package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.util.Arrays;
import java.util.List;

public class MssEventVolumeQueryConstants {

    public final static String NO_TYPE = "NO_TYPE";

    public final static String TECH_PACK_RAW_TABLES = "TECH_PACK_RAW_TABLES";

    final static String SUC_TABLE_NAME = "sucTable";

    final static String BLOCKED_TABLE_NAME = "blockTable";

    final static String DROPPED_TABLE_NAME = "dropTable";

    final static String ERR_TABLE_NAME = "errTable";

    final static String DATA_TABLE_NAME = "dataTable";

    final static String ALL_TABLE_NAME = "allTable";

    final static String AGG_COLUMN_VAR = "aggColumn";

    final static String GROUP_CONDITION_VAR = "groupCondition";

    final static String NODE_CONDITION_VAR = "nodeCondition";

    final static String NODE_NO_TYPE_CONDITION = "";

    final static String NODE_MSC_CONDITION = String.format("%s = :%s", EVENT_SOURCE_SQL_ID, EVENT_SOURCE_SQL_ID);

    final static String NODE_BSC_CONDITION = String.format("%s = :%s", CONTROLLER_SQL_ID, CONTROLLER_SQL_ID);

    final static String NODE_CELL_CONDITION = String.format("%s = :%s", CELL_SQL_ID, CELL_SQL_ID);

    final static String OIRGINAL_COLUMN_VAR = "originalSelectedColumns";

    final static String AGG_COLUMN_ALIAS_VAR = "aggColumnAlias";

    final static String WHERE_CONDITIONS_VAR = "whereConditions";

    final static String USE_EXCLUSIVE_TAC_VAR = "useExclusiveTac";

    final static String LEFT_TABLE_VAR = "leftTable";

    final static String RIGHT_TABLE_VAR = "rightTable";

    final static String LEFT_TABLE_SUBSQL_VAR = "leftTableSubsql";

    final static String RIGHT_TABLE_SUBSQL_VAR = "rightTableSubsql";

    final static String DATA_TABLE_SUBSQL_VAR = "data_table_subsql";

    final static String VOICE_TABLE_SUBSQL_VAR = "voice_table_subsql";

    final static String LOC_SERVICE_TABLE_SUBSQL_VAR = "loc_service_table_subsql";

    final static String IMSUB_TABLE_SUBSQL_VAR = "imsub_table_subsql";

    final static String DIFF_COLUMNS_VAR = "diffColumns";

    // Common Columns
    final static List<String> COMMON_COLUMNS_VOICE = Arrays.asList("tmp_st", "EVENT_ID", "TELE_SERVICE_CODE");

    final static List<String> COMMON_COLUMNS_LOC_SERVICE = Arrays.asList("tmp_st", "EVENT_ID");
    
    final static List<String> COMMON_COLUMNS_SMS = Arrays.asList("tmp_st", "EVENT_ID");

    // Original filter columns
    final static List<String> VOICE_ORIGINAL_COLUMNS_RAW = Arrays
            .asList("DATETIME_ID", "EVENT_ID", "TELE_SERVICE_CODE");

    final static List<String> ORIGINAL_COLUMNS_LOC_SERVICE_RAW = Arrays.asList("DATETIME_ID", "EVENT_ID");
    
    final static List<String> ORIGINAL_COLUMNS_SMS_RAW = Arrays.asList("DATETIME_ID", "EVENT_ID");

    // Original filter colums for aggregation table
    final static List<String> VOICE_ORIGINAL_COLUMNS_AGG_SUC = Arrays.asList("DATETIME_ID", "EVENT_ID",
            "NO_OF_SUCCESSES", "TELE_SERVICE_CODE");

    final static List<String> ORIGINAL_COLUMNS_LOC_SERVICE_AGG_SUC = Arrays.asList("DATETIME_ID", "EVENT_ID",
            "NO_OF_SUCCESSES");

    final static List<String> ORIGINAL_COLUMNS_SMS_AGG_SUC = Arrays.asList("DATETIME_ID", "EVENT_ID",
    "NO_OF_SUCCESSES");

    final static List<String> VOICE_ORIGINAL_COLUMNS_AGG_ERR = Arrays.asList("DATETIME_ID", "EVENT_ID", "NO_OF_ERRORS",
            "TELE_SERVICE_CODE");

    final static List<String> ORIGINAL_COLUMNS_LOC_SERVICE_AGG_ERR = Arrays.asList("DATETIME_ID", "EVENT_ID",
            "NO_OF_ERRORS");

    final static List<String> ORIGINAL_COLUMNS_SMS_AGG_ERR = Arrays.asList("DATETIME_ID", "EVENT_ID",
    "NO_OF_ERRORS");

    final static List<String> VOICE_WHERE_CONDITIONS = Arrays.asList("EVENT_ID >= 0", "EVENT_ID <= 3");

    final static List<String> LOC_SERVICE_WHERE_CONDITIONS = Arrays.asList("EVENT_ID = 6");

    final static List<String> SMS_WHERE_CONDITIONS = Arrays.asList("EVENT_ID = 4", "EVENT_ID = 5");

    // Original filter columns for impact subscriber
    final static List<String> ORIGINAL_COLUMNS_IMSI_RAW = Arrays.asList("DATETIME_ID", "IMSI");

    final static List<String> COMMON_COLUMNS_IMSI = Arrays.asList("tmp_st");

    final static String[] PARAMS_VALID_KEYS = { TIMERANGE_PARAM, TYPE_PARAM, TECH_PACK_TABLES, TECH_PACK_RAW_TABLES,
            START_TIME, END_TIME, INTERVAL_PARAM };

}
