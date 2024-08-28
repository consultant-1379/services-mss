package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.QueryConstructorConstants.*;
import static com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.Group;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryCreatorEnumConstants.MssTableType;
import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryCreatorEnumConstants.RawTableType;
import com.ericsson.eniq.events.server.templates.utils.TemplateUtils;
import com.ericsson.eniq.events.server.utils.Pair;

public abstract class MssEventVolumeQueryBaseCreator {

    @EJB
    protected TemplateUtils templateUtils;

    protected Map<String, Object> eventVolumeParameters;

    protected Map<MssTableType, Boolean> mssTableTypes;

    protected List<String> diffColumns;

    protected TechPackTables techPackTables;

    protected boolean useAggr;

    protected String type;

    protected boolean hasGroup;

    /**
     * setTemplateUtils is used for JUNIT
     * @param aTemplateUtil
     */
    public void setTemplateUtils(final TemplateUtils aTemplateUtils) {
        templateUtils = aTemplateUtils;
    }

    /**
     * createQuery is used to create query
     * @param templateParameters
     * @param checkValid            true -- check templateParameters false -- don't check templateParameters
     * @return
     */
    public String createQuery(final Map<String, Object> templateParameters,
            final Map<MssTableType, Boolean> aMssTableTypes, final boolean checkValid) {
        if (checkValid && !isValidTemplateParameters(templateParameters)) {
            return "";
        }
        populateCommonParameters(templateParameters);
        mssTableTypes = aMssTableTypes;
        return getQuery();
    }

    /**
     * createQuery is used to create query
     * @param templateParameters
     * @param aMssTableTypes
     * @return
     */
    public String createQuery(final Map<String, Object> templateParameters,
            final Map<MssTableType, Boolean> aMssTableTypes) {
        return createQuery(templateParameters, aMssTableTypes, false);
    }

    /**
     * ovveride this method to return query
     * @return query
     */
    protected abstract String getQuery();

    /**
     * createVoiceOriginalTable is used to create the voice table query
     * @param  mssTableType      it is voice, sms or loc_service
     * @param  voiceRawTableType it is a SUC, RAW or DROP_CALL table or view
     * @param  isEmergencyCall   it is emergency call or non emergency call
     * @param  aggrColumnAlias   it is aggregation column alias name
     * @param  tableAlias        it is table alias name
     * @return query of query from success table
     */
    protected String createOriginalTable(final MssTableType mssTableType, final RawTableType rawTableType,
            final List<String> commonColumns, final String aggrColumnAlias, final String tableAlias) {
        eventVolumeParameters.put(TABLES_VAR, getTables(mssTableType, rawTableType));
        eventVolumeParameters.put(COMMON_COLUMNS_VAR, commonColumns);
        eventVolumeParameters.put(FILTER_COLUMNS_VAR, getOriginalColumns(mssTableType, rawTableType));
        eventVolumeParameters.put(AGG_COLUMN_VAR, getAggColumn(rawTableType));
        eventVolumeParameters.put(WHERE_CONDITIONS_VAR, "");
        eventVolumeParameters.put(USE_EXCLUSIVE_TAC_VAR, !useAggr);
        final Pair<String, String> grpNodeCondition = getGroupConditionNodeCondition();
        eventVolumeParameters.put(GROUP_CONDITION_VAR, grpNodeCondition.first);
        eventVolumeParameters.put(NODE_CONDITION_VAR, grpNodeCondition.second);
        eventVolumeParameters.put(AGG_COLUMN_ALIAS_VAR, aggrColumnAlias);
        eventVolumeParameters.put(TABLES_ALIAS_VAR, tableAlias);
        return templateUtils.getQueryFromTemplate(MSS_EVENT_VOLUME_ORIGINAL_TEMPLATE, eventVolumeParameters);
    }

    /**
     * Populates eventVolumeParameters map that used to create query
     * @param templateParameters parameters required to generate query
     * @param allListInfoData provides info related to sub queries
     */
    protected void populateCommonParameters(final Map<String, Object> templateParameters) {
        diffColumns = new ArrayList<String>();
        eventVolumeParameters = new HashMap<String, Object>();
        eventVolumeParameters.putAll(templateParameters);
        hasGroup = eventVolumeParameters.containsKey(GROUP_DEFINITIONS);
        // don't check whether eventVolumeParameters.get() is null due to check it in isValidTemplateParameters
        useAggr = toUseAggregationTables((String) eventVolumeParameters.get(TIMERANGE_PARAM));
        type = (String) eventVolumeParameters.get(TYPE_PARAM);
        techPackTables = (TechPackTables) eventVolumeParameters.get(TECH_PACK_TABLES);
    }

    /**
     * isValidTemplateParameters is used to check whether templateParameters is valid or not
     * @param templateParameters
     * @return true -- valid false -- invalid
     */
    protected boolean isValidTemplateParameters(final Map<String, Object> templateParameters) {
        boolean rst = true;
        for (final String key : PARAMS_VALID_KEYS) {
            if (!templateParameters.containsKey(key) || templateParameters.get(key) == null) {
                rst = false;
                break;
            }
        }
        return rst;
    }

    /**
     * getOriginalColumns is used to get original columns according useAggr and raw table type
     * @param voiceRawTableType it is a SUC, RAW or DROP_CALL table or view
     * @return original columns
     */
    private List<String> getOriginalColumns(final MssTableType mssTableType, final RawTableType rawTypeType) {
        List<String> rst = VOICE_ORIGINAL_COLUMNS_RAW;
        if (MssTableType.VOICE == mssTableType) {
            if (!useAggr) {
                rst = VOICE_ORIGINAL_COLUMNS_RAW;
            } else {
                if (RawTableType.SUC == rawTypeType) {
                    rst = VOICE_ORIGINAL_COLUMNS_AGG_SUC;
                } else {
                    rst = VOICE_ORIGINAL_COLUMNS_AGG_ERR;
                }
            }
        } else if (MssTableType.LOC_SERVICE == mssTableType) {
            if (!useAggr) {
                rst = ORIGINAL_COLUMNS_LOC_SERVICE_RAW;
            } else {
                if (RawTableType.SUC == rawTypeType) {
                    rst = ORIGINAL_COLUMNS_LOC_SERVICE_AGG_SUC;
                } else {
                    rst = ORIGINAL_COLUMNS_LOC_SERVICE_AGG_ERR;
                }
            }
        } else if (MssTableType.SMS == mssTableType) {
            if (!useAggr) {
                rst = ORIGINAL_COLUMNS_SMS_RAW;
            } else {
                if (RawTableType.SUC == rawTypeType) {
                    rst = ORIGINAL_COLUMNS_SMS_AGG_SUC;
                } else {
                    rst = ORIGINAL_COLUMNS_SMS_AGG_ERR;
                }
            }
        }
        return rst;
    }

    /**
     * getAggColumn is used to get aggregation column according useAggr and raw table type
     * @param voiceRawTableType it is a SUC, RAW or DROP_CALL table or view
     * @return aggregation column
     */
    private String getAggColumn(final RawTableType voiceRawTableType) {
        String rst = NO_OF_SUC_ERR_RAW;
        if (!useAggr) {
            rst = NO_OF_SUC_ERR_RAW;
        } else {
            if (RawTableType.SUC == voiceRawTableType) {
                rst = SUM_OF_SUCCESSES_AGG;
            } else {
                rst = SUM_OF_ERRORS_AGG;
            }
        }
        return rst;
    }

    /**
     * getTables is used to get tables according to SUC, ERR or DROP_CALL
     * @parmm aggrType          denotes it is VOICE, SMS or LOC_SERVICE
     * @param voiceRawTableType denotes it is SUC, ERR or DROP_CALL
     * @return a list of tables
     */
    private List<String> getTables(final MssTableType tableType, final RawTableType voiceRawTableType) {
        List<String> rst = null;
        if (MssTableType.VOICE == tableType) {
            if (RawTableType.SUC == voiceRawTableType) {
                rst = techPackTables.getSucTables();
            } else if (RawTableType.DROP_CALL == voiceRawTableType) {
                rst = techPackTables.getErrTables(KEY_TYPE_DROP_CALL);
            } else {
                rst = techPackTables.getErrTables();
            }
        } else if (MssTableType.LOC_SERVICE == tableType) {
            if (RawTableType.SUC == voiceRawTableType) {
                rst = techPackTables.getSucTables(KEY_TYPE_LOC_SERVICE_SUC);
            } else {
                rst = techPackTables.getErrTables(KEY_TYPE_LOC_SERVICE_ERR);
            }
        } else if (MssTableType.SMS == tableType) {
            if (RawTableType.SUC == voiceRawTableType) {
                rst = techPackTables.getSucTables(KEY_TYPE_SMS_SUC);
            } else {
                rst = techPackTables.getErrTables(KEY_TYPE_SMS_ERR);
            }
        }
        return rst;
    }

    /**
     * getGroupConditionNodeCondition is used to get group or node condition
     * @return group or node condition 
     */
    @SuppressWarnings("unchecked")
    protected Pair<String, String> getGroupConditionNodeCondition() {
        String groupCondition = "";
        String nodeCondition = "";
        if (hasGroup) {
            final Map<String, Group> groupTypeDefs = Map.class.cast(eventVolumeParameters.get(GROUP_DEFINITIONS));
            groupCondition = getGroupCondition(groupTypeDefs);
        } else {
            if (NO_TYPE.equals(type)) {
                nodeCondition = NODE_NO_TYPE_CONDITION;
            } else if (TYPE_MSC.equals(type)) {
                nodeCondition = NODE_MSC_CONDITION;
            } else if (TYPE_BSC.equals(type)) {
                nodeCondition = NODE_BSC_CONDITION;
            } else if (TYPE_CELL.equals(type)) {
                nodeCondition = NODE_CELL_CONDITION;
            }
        }
        return new Pair<String, String>(groupCondition, nodeCondition);
    }

    /**
     * getGroupCondition is used to getGroupCondition
     * @param groupTypeDefs
     * @return
     * @throws MssEventVolumeQueryCreatorException
     */
    private String getGroupCondition(final Map<String, Group> groupTypeDefs) {
        String groupCondition = null;
        if (TYPE_MSC.equals(type)) {
            final Group group = groupTypeDefs.get(GROUP_TYPE_EVENT_SRC_CS);
            groupCondition = String.format("%s in (select %s from %s where GROUP_NAME = :GROUP_NAME)",
                    EVENT_SOURCE_SQL_ID, EVENT_SOURCE_SQL_ID, group.getTableName());
        } else if (TYPE_BSC.equals(type)) {
            final Group group = groupTypeDefs.get(GROUP_TYPE_HIER3);
            groupCondition = String.format("%s in (select %s from %s where GROUP_NAME = :GROUP_NAME)",
                    CONTROLLER_SQL_ID, CONTROLLER_SQL_ID, group.getTableName());
        } else if (TYPE_CELL.equals(type)) {
            final Group group = groupTypeDefs.get(GROUP_TYPE_HIER1);
            groupCondition = String.format("%s in (select %s from %s where GROUP_NAME = :GROUP_NAME)", CELL_SQL_ID,
                    CELL_SQL_ID, group.getTableName());
        }
        return groupCondition;
    }

    /**
     * This method is used to check if raw tables or the aggregation tables should be used
     * @param timeRange indicated the time range raw, 15 min or day tables
     * @return true if time range specifies the aggregation else false
     */
    private boolean toUseAggregationTables(final String timeRange) {
        boolean rst = false;
        if (EventDataSourceType.AGGREGATED_15MIN.getValue().equalsIgnoreCase(timeRange)
                || EventDataSourceType.AGGREGATED_DAY.getValue().equalsIgnoreCase(timeRange)) {
            rst = true;
        }
        return rst;
    }
}
