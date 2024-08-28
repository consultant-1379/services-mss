/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventanalysis.summary;

import java.util.ArrayList;
import java.util.List;

public class MssSummaryTemplateInfoObject {

    private List<String> commonColumns;

    private List<String> selectColumns;

    private List<String> joinColumns;

    private List<String> filterColumns;

    private List<String> typeToNodeTable;

    private List<String> eventDespTable;

    private List<String> eventDespCloumnNames;

    private List<String> extraTableJoin;

    private List<String> blockedAggTables;

    private List<String> droppedAggTables;

    private List<String> sucAggTables;

    private List<String> locServiceSucAggTables;

    private List<String> locServiceErrAggTables;

    private List<String> smsSucAggTables;

    private List<String> smsErrAggTables;

    private String extraTable;

    public MssSummaryTemplateInfoObject() {
        extraTable = "";
        extraTableJoin = new ArrayList<String>();
    }

    public List<String> getcommonColumns() {
        return commonColumns;
    }

    public void setcommonColumns(final List<String> commonColumns) {
        this.commonColumns = commonColumns;
    }

    public List<String> getSelectColumns() {
        return selectColumns;
    }

    public void setSelectColumns(final List<String> selectColumns) {
        this.selectColumns = selectColumns;
    }

    public List<String> getJoinColumns() {
        return joinColumns;
    }

    public void setJoinColumns(final List<String> joinColumns) {
        this.joinColumns = joinColumns;
    }

    public List<String> getFilterColumns() {
        return filterColumns;
    }

    public void setFilterColumns(final List<String> filterColumns) {
        this.filterColumns = filterColumns;
    }

    public List<String> getExtraTableJoin() {
        return extraTableJoin;
    }

    public void setExtraTableJoin(final List<String> extraTableJoin) {
        this.extraTableJoin = extraTableJoin;
    }

    public String getExtraTable() {
        return extraTable;
    }

    public void setExtraTable(final String extraTable) {
        this.extraTable = extraTable;
    }

    public List<String> getTypeToNodeTable() {
        return typeToNodeTable;
    }

    public void setTypeToNodeTable(final List<String> typeToNodeTable) {
        this.typeToNodeTable = typeToNodeTable;
    }

    public List<String> getEventDespTable() {
        return eventDespTable;
    }

    public void setEventDespTable(final List<String> eventDespTable) {
        this.eventDespTable = eventDespTable;
    }

    public List<String> getEventDespCloumnNames() {
        return eventDespCloumnNames;
    }

    public void setEventDespCloumnNames(final List<String> eventDespCloumnNames) {
        this.eventDespCloumnNames = eventDespCloumnNames;
    }

    public List<String> getBlockedAggTables() {
        return blockedAggTables;
    }

    public void setBlockedAggTables(final List<String> blockedAggTables) {
        this.blockedAggTables = blockedAggTables;
    }

    public List<String> getDroppedAggTables() {
        return droppedAggTables;
    }

    public void setDroppedAggTables(final List<String> droppedAggTables) {
        this.droppedAggTables = droppedAggTables;
    }

    public List<String> getSucAggTables() {
        return sucAggTables;
    }

    public void setSucAggTables(final List<String> sucAggTables) {
        this.sucAggTables = sucAggTables;
    }

    public List<String> getLocServiceSucAggTables() {
        return locServiceSucAggTables;
    }

    public void setLocServiceSucAggTables(final List<String> locServiceSucAggTables) {
        this.locServiceSucAggTables = locServiceSucAggTables;
    }

    public List<String> getLocServiceErrAggTables() {
        return locServiceErrAggTables;
    }

    public void setLocServiceErrAggTables(final List<String> locServiceErrAggTables) {
        this.locServiceErrAggTables = locServiceErrAggTables;
    }

    public List<String> getSmsSucAggTables() {
        return smsSucAggTables;
    }

    public void setSmsSucAggTables(final List<String> smsSucAggTables) {
        this.smsSucAggTables = smsSucAggTables;
    }

    public List<String> getSmsErrAggTables() {
        return smsErrAggTables;
    }

    public void setSmsErrAggTables(final List<String> smsErrAggTables) {
        this.smsErrAggTables = smsErrAggTables;
    }
}
