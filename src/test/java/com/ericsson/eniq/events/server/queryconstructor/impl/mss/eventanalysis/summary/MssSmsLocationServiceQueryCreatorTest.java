package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static org.junit.Assert.assertNotNull;

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

public class MssSmsLocationServiceQueryCreatorTest extends QueryConstructorBaseTestClass {

    MssSmsAndLocationServiceSummaryQueryCreator testObj;

    private Map<String, Object> params;

    private Map<String, GroupHashId> groups = new HashMap<String, GroupHashId>();

    @Before
    public void onSetUp() {
        testObj = new MssSmsAndLocationServiceSummaryQueryCreator();
        testObj.setTempInfoToTypeMappings(this.sumTempInfoToTypeMappings);
        testObj.setQueryConstructorUtils(this.queryConstructorUtils);
        populateGropDef();
        params = new HashMap<String, Object>();
        params.put(GROUP_DEFINITIONS, groups);
        params.put(START_TIME, "'2011-03-15 00:00:00'");
        params.put(END_TIME, "'2011-03-16 00:00:00'");
        params.put(TIMERANGE_PARAM, "TR_1");
        params.put(TECH_PACK_TABLES, populateTechPackTablesData());
        params.put(IS_EXCULDED_TAC_OR_TACGROUP, true);

    }

    private TechPackTables populateTechPackTablesData() {
        final TechPackTables techPackTables = new TechPackTables(TableType.RAW);
        final TechPack techPack = new TechPack("EVENT_E_MSS", TableType.RAW, "DIM_E_MSS");
        techPackTables.addTechPack(techPack);

        final List<String> locServiceSucTablesRaw = new ArrayList<String>();
        locServiceSucTablesRaw.add(EVENT_E_MSS_LOC_SERVICE_CDR_SUC_RAW);
        final List<String> locServiceErrTablesRaw = new ArrayList<String>();
        locServiceErrTablesRaw.add(EVENT_E_MSS_LOC_SERVICE_CDR_ERR_RAW);

        techPack.setErrRawTables(KEY_TYPE_LOC_SERVICE_ERR, locServiceErrTablesRaw);
        techPack.setSucRawTables(KEY_TYPE_LOC_SERVICE_SUC, locServiceSucTablesRaw);

        final List<String> smsSucTablesRaw = new ArrayList<String>();
        smsSucTablesRaw.add(EVENT_E_MSS_SMS_CDR_SUC_RAW);
        final List<String> smsErrTablesRaw = new ArrayList<String>();
        smsErrTablesRaw.add(EVENT_E_MSS_SMS_CDR_ERR_RAW);

        techPack.setErrRawTables(KEY_TYPE_SMS_ERR, smsErrTablesRaw);
        techPack.setSucRawTables(KEY_TYPE_SMS_SUC, smsSucTablesRaw);

        return techPackTables;
    }

    private void populateGropDef() {
        final String tpName = "dc_e_abc";
        final String testVersionId = tpName + ":((123))";
        final int isNullable = 0;
        final String tac = "TAC";
        final String imsi = "IMSI";
        final String eventSrc = "EVNTSRC_CS";
        final String controller = "RAT_VEND_HIER3";
        final String cell = "RAT_VEND_HIER321";
        final Grouptypes key1 = getKey(tac, tac, "varchar", 64, 0, testVersionId, isNullable);
        final List<Grouptypes> keys1 = Arrays.asList(key1);
        final GroupTypeDef def1 = new GroupTypeDef(keys1, tac, testVersionId);
        final GroupHashId velocityGroupDefinition1 = getCreateVelocityGroup(def1);
        final Grouptypes key2 = getKey(imsi, imsi, "varchar", 64, 0, testVersionId, isNullable);
        final List<Grouptypes> keys2 = Arrays.asList(key2);
        final GroupTypeDef def2 = new GroupTypeDef(keys2, imsi, testVersionId);
        final GroupHashId velocityGroupDefinition2 = getCreateVelocityGroup(def2);
        final Grouptypes key3 = getKey(eventSrc, "EVNTSRC_ID", "varchar", 64, 0, testVersionId, isNullable);
        final List<Grouptypes> keys3 = Arrays.asList(key3);
        final GroupTypeDef def3 = new GroupTypeDef(keys3, eventSrc, testVersionId);
        final GroupHashId velocityGroupDefinition3 = getCreateVelocityGroup(def3);
        final Grouptypes key4 = getKey(controller, "HIERARCHY_3", "varchar", 64, 0, testVersionId, isNullable);
        final Grouptypes key41 = getKey(controller, "VENDOR", "varchar", 64, 0, testVersionId, isNullable);
        final Grouptypes key42 = getKey(controller, "RAT", "tinyint", 64, 0, testVersionId, isNullable);
        final Grouptypes key43 = getKey(controller, "HIER3_ID", "varchar", 64, 0, testVersionId, isNullable);
        final List<Grouptypes> keys4 = Arrays.asList(key4, key41, key42, key43);
        final GroupTypeDef def4 = new GroupTypeDef(keys4, controller, testVersionId);
        final GroupHashId velocityGroupDefinition4 = getCreateVelocityGroup(def4);
        final Grouptypes key5 = getKey(cell, "HIERARCHY_321", "varchar", 64, 0, testVersionId, isNullable);
        final Grouptypes key51 = getKey(cell, "HIERARCHY_3", "varchar", 64, 0, testVersionId, isNullable);
        final Grouptypes key52 = getKey(cell, "VENDOR", "varchar", 64, 0, testVersionId, isNullable);
        final Grouptypes key53 = getKey(cell, "RAT", "tinyint", 64, 0, testVersionId, isNullable);
        final Grouptypes key54 = getKey(cell, "HIER321_ID", "varchar", 64, 0, testVersionId, isNullable);
        final List<Grouptypes> keys5 = Arrays.asList(key5, key51, key52, key53, key54);
        final GroupTypeDef def5 = new GroupTypeDef(keys5, cell, testVersionId);
        final GroupHashId velocityGroupDefinition5 = getCreateVelocityGroup(def5);
        groups.put(def1.getGroupType(), velocityGroupDefinition1);
        groups.put(def2.getGroupType(), velocityGroupDefinition2);
        groups.put(def3.getGroupType(), velocityGroupDefinition3);
        groups.put(def4.getGroupType(), velocityGroupDefinition4);
        groups.put(def5.getGroupType(), velocityGroupDefinition5);
    }

    private Grouptypes getKey(final String gpType, final String name, final String type, final int size,
            final int scale, final String vId, int nullable) {
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
    public void testGetMssLocationServiceSummaryQueryForRawTAC() {
        params.put(TYPE_PARAM, TAC_PARAM.toUpperCase());
        testObj.setToCreateSmsQuery(false);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssLocationServiceSummaryQueryForRawMan() {
        params.put(TYPE_PARAM, MAN_PARAM.toUpperCase());
        testObj.setToCreateSmsQuery(false);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssLocationServiceSummaryQueryForRawController() {
        params.put(TYPE_PARAM, BSC_PARAM.toUpperCase());
        testObj.setToCreateSmsQuery(false);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssLocationServiceSummaryQueryForRawAccessArea() {
        params.put(TYPE_PARAM, CELL_PARAM.toUpperCase());
        testObj.setToCreateSmsQuery(false);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssLocationServiceSummaryQueryForRawEventSrc() {
        params.put(TYPE_PARAM, MSC_PARAM.toUpperCase());
        testObj.setToCreateSmsQuery(false);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssLocationServiceSummaryQueryForGroupRawEventSrc() {
        params.put(TYPE_PARAM, MSC_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "mscGroup");
        testObj.setToCreateSmsQuery(false);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssLocationServiceSummaryQueryForGroupRawTAC() {
        params.put(TYPE_PARAM, TAC_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "tacGroup");
        testObj.setToCreateSmsQuery(false);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssLocationServiceSummaryQueryForGroupRawController() {
        params.put(TYPE_PARAM, BSC_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "bscGroup");
        testObj.setToCreateSmsQuery(false);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssLocationServiceSummaryQueryForGroupRawAccessArea() {
        params.put(TYPE_PARAM, CELL_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "cellGroup");
        testObj.setToCreateSmsQuery(false);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssLocationServiceSummaryQueryForGroupSummaryDrillDownRawController() {
        params.put(TYPE_PARAM, BSC_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "bscGroup");
        params.put(EVENT_ID_PARAM, "6");
        testObj.setToCreateSmsQuery(false);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssLocationServiceSummaryQueryForGroupSummaryDrillDownRawEventSrc() {
        params.put(TYPE_PARAM, MSC_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "mscGroup");
        params.put(EVENT_ID_PARAM, "6");
        testObj.setToCreateSmsQuery(false);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssLocationServiceSummaryQueryForGroupSummaryDrillDownRawAccessArea() {
        params.put(TYPE_PARAM, CELL_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "cellGroup");
        params.put(EVENT_ID_PARAM, "6");
        testObj.setToCreateSmsQuery(false);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssLocationServiceSummaryQueryForGroupSummaryDrillDownTac() {
        params.put(TYPE_PARAM, TAC_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "tacGroup");
        params.put(EVENT_ID_PARAM, "6");
        testObj.setToCreateSmsQuery(false);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSmsSummaryQueryForRawTAC() {
        params.put(TYPE_PARAM, TAC_PARAM.toUpperCase());
        testObj.setToCreateSmsQuery(true);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSmsSummaryQueryForRawMan() {
        params.put(TYPE_PARAM, MAN_PARAM.toUpperCase());
        testObj.setToCreateSmsQuery(true);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSmsSummaryQueryForRawController() {
        params.put(TYPE_PARAM, BSC_PARAM.toUpperCase());
        testObj.setToCreateSmsQuery(true);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSmsSummaryQueryForRawAccessArea() {
        params.put(TYPE_PARAM, CELL_PARAM.toUpperCase());
        testObj.setToCreateSmsQuery(true);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSmsSummaryQueryForRawEventSrc() {
        params.put(TYPE_PARAM, MSC_PARAM.toUpperCase());
        testObj.setToCreateSmsQuery(true);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSmsSummaryQueryForGroupRawEventSrc() {
        params.put(TYPE_PARAM, MSC_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "mscGroup");
        testObj.setToCreateSmsQuery(true);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSmsSummaryQueryForGroupRawTAC() {
        params.put(TYPE_PARAM, TAC_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "tacGroup");
        testObj.setToCreateSmsQuery(true);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSmsSummaryQueryForGroupRawController() {
        params.put(TYPE_PARAM, BSC_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "bscGroup");
        testObj.setToCreateSmsQuery(true);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSmsSummaryQueryForGroupRawAccessArea() {
        params.put(TYPE_PARAM, CELL_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "cellGroup");
        testObj.setToCreateSmsQuery(true);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSmsSummaryQueryForGroupSummaryDrillDownRawController() {
        params.put(TYPE_PARAM, BSC_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "bscGroup");
        params.put(EVENT_ID_PARAM, "4");
        testObj.setToCreateSmsQuery(true);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSmsSummaryQueryForGroupSummaryDrillDownRawEventSrc() {
        params.put(TYPE_PARAM, MSC_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "mscGroup");
        params.put(EVENT_ID_PARAM, "4");
        testObj.setToCreateSmsQuery(true);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSmsSummaryQueryForGroupSummaryDrillDownRawAccessArea() {
        params.put(TYPE_PARAM, CELL_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "cellGroup");
        params.put(EVENT_ID_PARAM, "4");
        testObj.setToCreateSmsQuery(true);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }

    @Test
    public void testGetMssSmsSummaryQueryForGroupSummaryDrillDownRawTac() {
        params.put(TYPE_PARAM, TAC_PARAM.toUpperCase());
        params.put(GROUP_NAME_KEY, "tacGroup");
        params.put(EVENT_ID_PARAM, "4");
        testObj.setToCreateSmsQuery(true);
        final String sql = testObj.createQuery(params);
        System.out.println(sql);
        assertNotNull(sql);
    }
}
