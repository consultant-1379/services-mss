/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Singleton;
import javax.ejb.Startup;

import com.ericsson.eniq.events.server.common.tablesandviews.MssAggregationView;

@Singleton
@Startup
@ConcurrencyManagement(ConcurrencyManagementType.CONTAINER)
@Lock(LockType.WRITE)
public class MssSummaryTemplateInfoToTypeMappings {

    private List<String> evntDespTable;

    private List<String> evntTypeDespColumns;

    private List<String> nodeToTypeTablesHier321;

    private List<String> nodeToTypeTablesEvntSrc;

    private List<String> nodeToTypeTablesTacManuf;

    private MssSummaryTemplateInfoObject mscType;

    private MssSummaryTemplateInfoObject controller;

    private MssSummaryTemplateInfoObject cell;

    private MssSummaryTemplateInfoObject tac;

    private MssSummaryTemplateInfoObject manuf;

    private MssSummaryTemplateInfoObject imsi;

    private MssSummaryTemplateInfoObject msisdn;

    @PostConstruct
    public void loadAllTemplateInfo() {
        loadEvntDespTableList();
        loadEventDespColumnNameList();
        loadTopologyTableList();
        loadMscTypeTemplateInfo();
        loadControllerTypeTemplateInfo();
        loadCellTypeTemplateInfo();
        loadTacTypeTemplateInfo();
        loadManufacTypeTemplateInfo();
        loadImsiTypeTemplateInfo();
        loadMsisdnTypeTemplateInfo();
    }

    @PreDestroy
    public void releaseAllResource() {
        evntDespTable.clear();
        evntTypeDespColumns.clear();
        nodeToTypeTablesHier321.clear();
        nodeToTypeTablesEvntSrc.clear();
        nodeToTypeTablesTacManuf.clear();
    }

    private void loadEvntDespTableList() {
        evntDespTable = new ArrayList<String>();
        evntDespTable.add(DIM_E_MSS_EVNTTYPE);
    }

    private void loadEventDespColumnNameList() {
        evntTypeDespColumns = new ArrayList<String>();
        evntTypeDespColumns.add(MSS_EVNTTYPE_DESCRIPTION_COLUMN);
    }

    private void loadTopologyTableList() {
        nodeToTypeTablesTacManuf = new ArrayList<String>();
        nodeToTypeTablesTacManuf.add(DIM_E_SGEH_TAC);
        nodeToTypeTablesHier321 = new ArrayList<String>();
        nodeToTypeTablesHier321.add(DIM_E_SGEH_HIER321);
        nodeToTypeTablesHier321.add(DIM_Z_SGEH_HIER321);
        nodeToTypeTablesEvntSrc = new ArrayList<String>();
        nodeToTypeTablesEvntSrc.add(DIM_E_MSS_EVNTSRC);
    }

    private void loadMscTypeTemplateInfo() {
        mscType = new MssSummaryTemplateInfoObject();
        mscType.setcommonColumns(MssSummaryQueryCreatorColumnInfo.COMMON_COLUMNS.getColumnInfo());
        mscType.setJoinColumns(MssSummaryQueryCreatorColumnInfo.MSC_MSCGROUP_JOIN_COLUMNS.getColumnInfo());
        mscType.setFilterColumns(MssSummaryQueryCreatorColumnInfo.MSC_MSCGROUP_FILTER_COLUMNS.getColumnInfo());
        mscType.setSelectColumns(MssSummaryQueryCreatorColumnInfo.MSC_SELECT_COLUMNS.getColumnInfo());
        mscType.setEventDespTable(evntDespTable);
        mscType.setEventDespCloumnNames(evntTypeDespColumns);
        mscType.setTypeToNodeTable(getTopologyTable(TYPE_MSC));
        mscType.setBlockedAggTables(MssAggregationView.EVENTSRC_SUMMARY.getErrorView());
        mscType.setDroppedAggTables(MssAggregationView.EVENTSRC_SUMMARY.getDroppedView());
        mscType.setSucAggTables(MssAggregationView.EVENTSRC_SUMMARY.getSucessView());
        mscType.setLocServiceSucAggTables(MssAggregationView.LOC_SERVICE_EVENTSRC_SUMMARY.getSucessView());
        mscType.setLocServiceErrAggTables(MssAggregationView.LOC_SERVICE_EVENTSRC_SUMMARY.getErrorView());
        mscType.setSmsSucAggTables(MssAggregationView.SMS_EVENTSRC_SUMMARY.getSucessView());
        mscType.setSmsErrAggTables(MssAggregationView.SMS_EVENTSRC_SUMMARY.getErrorView());
    }

    private void loadControllerTypeTemplateInfo() {
        controller = new MssSummaryTemplateInfoObject();
        controller.setcommonColumns(MssSummaryQueryCreatorColumnInfo.COMMON_COLUMNS.getColumnInfo());
        controller.setJoinColumns(MssSummaryQueryCreatorColumnInfo.CONTROLLER_CONTROLLERGROUP_HIER321_JOIN_COLUMNS
                .getColumnInfo());
        controller.setFilterColumns(MssSummaryQueryCreatorColumnInfo.CONTROLLER_CONTROLLERGROUP_FILTER_COLUMNS
                .getColumnInfo());
        controller.setSelectColumns(MssSummaryQueryCreatorColumnInfo.CONTROLLER_HIER321_SELECT_COLUMNS.getColumnInfo());
        controller.setEventDespTable(evntDespTable);
        controller.setEventDespCloumnNames(evntTypeDespColumns);
        controller.setTypeToNodeTable(getTopologyTable(TYPE_BSC));
        controller.setBlockedAggTables(MssAggregationView.CONTROLLER_CELL_SUMMARY.getErrorView());
        controller.setDroppedAggTables(MssAggregationView.CONTROLLER_CELL_SUMMARY.getDroppedView());
        controller.setSucAggTables(MssAggregationView.CONTROLLER_CELL_SUMMARY.getSucessView());
        controller.setLocServiceSucAggTables(MssAggregationView.LOC_SERVICE_CONTROLLER_CELL_SUMMARY.getSucessView());
        controller.setLocServiceErrAggTables(MssAggregationView.LOC_SERVICE_CONTROLLER_CELL_SUMMARY.getErrorView());
        controller.setSmsSucAggTables(MssAggregationView.SMS_CONTROLLER_CELL_SUMMARY.getSucessView());
        controller.setSmsErrAggTables(MssAggregationView.SMS_CONTROLLER_CELL_SUMMARY.getErrorView());
    }

    private void loadCellTypeTemplateInfo() {
        cell = new MssSummaryTemplateInfoObject();
        cell.setcommonColumns(MssSummaryQueryCreatorColumnInfo.COMMON_COLUMNS.getColumnInfo());
        cell.setJoinColumns(MssSummaryQueryCreatorColumnInfo.CELL_CELLGROUP_HIER321_JOIN_COLUMNS.getColumnInfo());
        cell.setFilterColumns(MssSummaryQueryCreatorColumnInfo.CELL_CELLGROUP_FILTER_COLUMNS.getColumnInfo());
        cell.setSelectColumns(MssSummaryQueryCreatorColumnInfo.CELL_HIER321_SELECT_COLUMNS.getColumnInfo());
        cell.setEventDespTable(evntDespTable);
        cell.setEventDespCloumnNames(evntTypeDespColumns);
        cell.setTypeToNodeTable(getTopologyTable(TYPE_CELL));
        cell.setBlockedAggTables(MssAggregationView.CONTROLLER_CELL_SUMMARY.getErrorView());
        cell.setDroppedAggTables(MssAggregationView.CONTROLLER_CELL_SUMMARY.getDroppedView());
        cell.setSucAggTables(MssAggregationView.CONTROLLER_CELL_SUMMARY.getSucessView());
        cell.setLocServiceSucAggTables(MssAggregationView.LOC_SERVICE_CONTROLLER_CELL_SUMMARY.getSucessView());
        cell.setLocServiceErrAggTables(MssAggregationView.LOC_SERVICE_CONTROLLER_CELL_SUMMARY.getErrorView());
        cell.setSmsSucAggTables(MssAggregationView.SMS_CONTROLLER_CELL_SUMMARY.getSucessView());
        cell.setSmsErrAggTables(MssAggregationView.SMS_CONTROLLER_CELL_SUMMARY.getErrorView());
    }

    private void loadTacTypeTemplateInfo() {
        tac = new MssSummaryTemplateInfoObject();
        tac.setcommonColumns(MssSummaryQueryCreatorColumnInfo.COMMON_COLUMNS.getColumnInfo());
        tac.setJoinColumns(MssSummaryQueryCreatorColumnInfo.TAC_TACGROUP_JOIN_COLUMNS.getColumnInfo());
        tac.setFilterColumns(MssSummaryQueryCreatorColumnInfo.TAC_TACGROUP_MANUFAC_FILTER_COLUMNS.getColumnInfo());
        tac.setSelectColumns(MssSummaryQueryCreatorColumnInfo.TAC_SELECT_COLUMNS.getColumnInfo());
        tac.setEventDespTable(evntDespTable);
        tac.setEventDespCloumnNames(evntTypeDespColumns);
        tac.setTypeToNodeTable(getTopologyTable(TYPE_TAC));
        tac.setBlockedAggTables(MssAggregationView.TAC_MANUFACTURER_SUMMARY.getErrorView());
        tac.setDroppedAggTables(MssAggregationView.TAC_MANUFACTURER_SUMMARY.getDroppedView());
        tac.setSucAggTables(MssAggregationView.TAC_MANUFACTURER_SUMMARY.getSucessView());
        tac.setLocServiceSucAggTables(MssAggregationView.LOC_SERVICE_TAC_MANUFACTURER_SUMMARY.getSucessView());
        tac.setLocServiceErrAggTables(MssAggregationView.LOC_SERVICE_TAC_MANUFACTURER_SUMMARY.getErrorView());
        tac.setSmsSucAggTables(MssAggregationView.SMS_TAC_MANUFACTURER_SUMMARY.getSucessView());
        tac.setSmsErrAggTables(MssAggregationView.SMS_TAC_MANUFACTURER_SUMMARY.getErrorView());
    }

    private void loadManufacTypeTemplateInfo() {
        manuf = new MssSummaryTemplateInfoObject();
        manuf.setcommonColumns(MssSummaryQueryCreatorColumnInfo.COMMON_COLUMNS.getColumnInfo());
        manuf.setJoinColumns(MssSummaryQueryCreatorColumnInfo.MANUFAC_JOIN_COLUMNS.getColumnInfo());
        manuf.setFilterColumns(MssSummaryQueryCreatorColumnInfo.TAC_TACGROUP_MANUFAC_FILTER_COLUMNS.getColumnInfo());
        manuf.setSelectColumns(MssSummaryQueryCreatorColumnInfo.MANUFAC_SELECT_COLUMNS.getColumnInfo());
        manuf.setEventDespTable(evntDespTable);
        manuf.setEventDespCloumnNames(evntTypeDespColumns);
        manuf.setTypeToNodeTable(getTopologyTable(TYPE_MAN));
        manuf.setExtraTable(DIM_E_SGEH_TAC);
        manuf.setExtraTableJoin(MssSummaryQueryCreatorColumnInfo.MANUF_EXTRA_TABLE_JOIN_COLUMN.getColumnInfo());
        manuf.setBlockedAggTables(MssAggregationView.TAC_MANUFACTURER_SUMMARY.getErrorView());
        manuf.setDroppedAggTables(MssAggregationView.TAC_MANUFACTURER_SUMMARY.getDroppedView());
        manuf.setSucAggTables(MssAggregationView.TAC_MANUFACTURER_SUMMARY.getSucessView());
        manuf.setLocServiceSucAggTables(MssAggregationView.LOC_SERVICE_TAC_MANUFACTURER_SUMMARY.getSucessView());
        manuf.setLocServiceErrAggTables(MssAggregationView.LOC_SERVICE_TAC_MANUFACTURER_SUMMARY.getErrorView());
        manuf.setSmsSucAggTables(MssAggregationView.SMS_TAC_MANUFACTURER_SUMMARY.getSucessView());
        manuf.setSmsErrAggTables(MssAggregationView.SMS_TAC_MANUFACTURER_SUMMARY.getErrorView());
    }

    private void loadImsiTypeTemplateInfo() {
        imsi = new MssSummaryTemplateInfoObject();
        imsi.setSelectColumns(MssSummaryQueryCreatorColumnInfo.IMSI_JOIN_COLUMN.getColumnInfo());
        imsi.setcommonColumns(MssSummaryQueryCreatorColumnInfo.COMMON_COLUMNS.getColumnInfo());
        imsi.setJoinColumns(MssSummaryQueryCreatorColumnInfo.IMSI_JOIN_COLUMN.getColumnInfo());
        imsi.setFilterColumns(MssSummaryQueryCreatorColumnInfo.IMSI_GROUP_FILTER_COLUMNS.getColumnInfo());
        imsi.setEventDespTable(evntDespTable);
        imsi.setEventDespCloumnNames(evntTypeDespColumns);
    }

    private void loadMsisdnTypeTemplateInfo() {
        msisdn = new MssSummaryTemplateInfoObject();
        msisdn.setSelectColumns(MssSummaryQueryCreatorColumnInfo.MSISDN_JOIN_COLUMN.getColumnInfo());
        msisdn.setcommonColumns(MssSummaryQueryCreatorColumnInfo.COMMON_COLUMNS.getColumnInfo());
        msisdn.setJoinColumns(MssSummaryQueryCreatorColumnInfo.MSISDN_JOIN_COLUMN.getColumnInfo());
        msisdn.setFilterColumns(MssSummaryQueryCreatorColumnInfo.MSISDN_GROUP_FILTER_COLUMNS.getColumnInfo());
        msisdn.setEventDespTable(evntDespTable);
        msisdn.setEventDespCloumnNames(evntTypeDespColumns);
    }

    private List<String> getTopologyTable(final String type) {
        if (TYPE_TAC.equals(type) || TYPE_MAN.equals(type)) {
            return nodeToTypeTablesTacManuf;
        } else if (TYPE_BSC.equals(type) || TYPE_CELL.equals(type)) {
            return nodeToTypeTablesHier321;
        } else if (TYPE_MSC.equals(type)) {
            return nodeToTypeTablesEvntSrc;
        }
        return null;
    }

    @Lock(LockType.READ)
    public MssSummaryTemplateInfoObject getTemplateInfoObj(final String type) {
        if (TYPE_TAC.equals(type)) {
            return tac;
        } else if (TYPE_MAN.equals(type)) {
            return manuf;
        } else if (TYPE_BSC.equals(type)) {
            return controller;
        } else if (TYPE_MSC.equals(type)) {
            return mscType;
        } else if (TYPE_CELL.equals(type)) {
            return cell;
        } else if (TYPE_IMSI.equals(type)) {
            return imsi;
        } else if (TYPE_MSISDN.equals(type)) {
            return msisdn;
        }
        return null;
    }
}
