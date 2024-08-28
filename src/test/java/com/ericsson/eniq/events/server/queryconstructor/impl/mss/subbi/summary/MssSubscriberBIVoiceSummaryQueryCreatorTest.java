package com.ericsson.eniq.events.server.queryconstructor.impl.mss.subbi.summary;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.repository.cache.GroupTypeDef;
import com.distocraft.dc5000.repository.cache.GroupTypeKeyDef;
import com.distocraft.dc5000.repository.dwhrep.Grouptypes;
import com.ericsson.eniq.events.server.common.GroupHashId;
import com.ericsson.eniq.events.server.common.tablesandviews.TableType;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPack;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.queryconstructor.QueryConstructorBaseTestClass;

public class MssSubscriberBIVoiceSummaryQueryCreatorTest extends QueryConstructorBaseTestClass {

    MssSubscriberBISummaryQueryCreator testObj;

    private Map<String, Object> params;

    private final Map<String, GroupHashId> groups = new HashMap<String, GroupHashId>();

    @Before
    public void onSetUp() {
        testObj = new MssSubscriberBIVoiceSummaryQueryCreator();
        testObj.setQueryConstructorUtils(this.queryConstructorUtils);
        populateGropDef();
        final List<String> sucTables = new ArrayList<String>();
        sucTables.add(EVENT_E_MSS_VOICE_CDR_SUC_RAW);
        params = new HashMap<String, Object>();
        params.put(GROUP_DEFINITIONS, groups);
        params.put(RAW_SUC_TABLES, sucTables);
        params.put(START_TIME, "'2011-03-15 00:00:00'");
        params.put(END_TIME, "'2011-03-16 00:00:00'");
        params.put(TIMERANGE_PARAM, "TR_3");
        params.put(TECH_PACK_TABLES, populateTechPackTablesData());
        params.put(IS_EXCULDED_TAC_OR_TACGROUP, true);
    }

    private TechPackTables populateTechPackTablesData() {
        final TechPackTables techPackTables = new TechPackTables(TableType.RAW);
        final TechPack techPack = new TechPack("EVENT_E_MSS", TableType.RAW, "DIM_E_MSS");
        techPackTables.addTechPack(techPack);
        final List<String> sucTablesRaw = new ArrayList<String>();
        sucTablesRaw.add(EVENT_E_MSS_VOICE_CDR_SUC_RAW);
        final List<String> errTablesRawDropped = new ArrayList<String>();
        errTablesRawDropped.add(EVENT_E_MSS_VOICE_CDR_DROP_CALL_RAW);
        final List<String> errTablesRawBlocked = new ArrayList<String>();
        errTablesRawBlocked.add(EVENT_E_MSS_VOICE_CDR_ERR_RAW);
        techPack.setErrRawTables(KEY_TYPE_ERR, errTablesRawBlocked);
        techPack.setErrRawTables(KEY_TYPE_DROP_CALL, errTablesRawDropped);
        techPack.setSucRawTables(sucTablesRaw);
        return techPackTables;
    }

    private void populateGropDef() {
        final String tpName = "dc_e_abc";
        final String testVersionId = tpName + ":((123))";
        final int isNullable = 0;
        final String imsi = "IMSI";

        final Grouptypes key = getKey(imsi, imsi, "varchar", 64, 0, testVersionId, isNullable);
        final List<Grouptypes> keys = Arrays.asList(key);
        final GroupTypeDef def = new GroupTypeDef(keys, imsi, testVersionId);
        final GroupHashId velocityGroupDefinition2 = getCreateVelocityGroup(def);
        groups.put(def.getGroupType(), velocityGroupDefinition2);
    }

    private Grouptypes getKey(final String gpType, final String name, final String type, final int size,
            final int scale, final String vId, final int nullable) {
        final Grouptypes key = new Grouptypes(null);
        key.setGrouptype(gpType);
        key.setDataname(name);
        key.setDatatype(type);
        key.setDatasize(size);
        key.setDatascale(scale);
        key.setVersionid(vId);
        key.setNullable(nullable);
        return key;
    }

    private GroupHashId getCreateVelocityGroup(final GroupTypeDef aGroupDefinition) {
        final List<String> dataKeyNames = getDataNames(aGroupDefinition);
        return new GroupHashId(aGroupDefinition.getGroupType(), aGroupDefinition.getTableName(),
                GroupTypeDef.KEY_NAME_GROUP_NAME, dataKeyNames);
    }

    private List<String> getDataNames(final GroupTypeDef aGroupDefinition) {
        // I'm still creating them in a loop......
        final List<String> dataKeyNames = new ArrayList<String>();
        for (final GroupTypeKeyDef groupKey : aGroupDefinition.getDataKeys()) {
            //Ignore HIERARCHY_2 for 3G
            if (!groupKey.getKeyName().equals(HIER2)) {
                dataKeyNames.add(groupKey.getKeyName());
            }
        }
        return dataKeyNames;
    }

    @Test
    public void testGetMssSubbiQueryForFailures() {
        final List<String> sql = testObj.createFailureQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSubbiQueryForGroupFailures() {
        params.put(GROUP_NAME_KEY, "IMSIGroup1");
        params.put(TYPE_PARAM, TYPE_IMSI);
        final List<String> sql = testObj.createFailureQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSubbiQueryForBusyHour() {
        params.put(TYPE_PARAM, TYPE_IMSI);
        final List<String> sql = testObj.createBusyHourQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSubbiQueryForGroupBusyHour() {
        params.put(GROUP_NAME_KEY, "IMSIGroup1");
        params.put(TYPE_PARAM, TYPE_IMSI);
        final List<String> sql = testObj.createBusyHourQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSubbiQueryForBusyDay() {
        params.put(TZ_OFFSET, "60");
        params.put(TYPE_PARAM, TYPE_IMSI);
        final List<String> sql = testObj.createBusyDayQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSubbiQueryForGroupBusyDay() {
        params.put(TZ_OFFSET, "60");
        params.put(TYPE_PARAM, TYPE_IMSI);
        params.put(GROUP_NAME_KEY, "IMSIGroup1");
        final List<String> sql = testObj.createBusyDayQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSubbiQueryForCellAnalysis() {
        params.put(TZ_OFFSET, "60");
        params.put(TYPE_PARAM, TYPE_IMSI);
        final List<String> sql = testObj.createCellAnalysisQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSubbiQueryForTerminalAnalysis() {
        params.put(TZ_OFFSET, "60");
        params.put(TYPE_PARAM, TYPE_IMSI);
        final List<String> sql = testObj.createTerminalAnalysisQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }
}
