/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.*;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.common.TechPackData;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.queryconstructor.QueryConstructor;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * 
 * Sub-resource for MSS event analysis detail and summary request handling.
 * 
 * @author egraman
 * @author echimma
 * @since 2011
 * 
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class MssEventAnalysisResource extends MssBaseResource {

    @EJB
    protected QueryConstructor queryConstructor;

    public static enum FTYPE_ENUM {
        BLOCKED, DROPPED
    };

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax .ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(NODE_PARAM) || requestParameters.containsKey(GROUP_NAME_PARAM) || requestParameters.containsKey(IMSI_PARAM)
                || requestParameters.containsKey(MSISDN_PARAM)) {
            if (!queryUtils.checkValidValue(requestParameters)) {
                return false;
            }
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources.BaseResource#getData(java.lang .String, javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters) throws WebApplicationException {
        // TODO Auto-generated method stub
        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        checkAndCreateINFOAuditLogEntryForURI(requestParameters);

        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(GRID_PARAM)) {
            return getGridResults(requestId, requestParameters);
        }

        return getNoSuchDisplayErrorResponse(displayType);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources.BaseResource#checkParameters( javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected List<String> checkParameters(final MultivaluedMap<String, String> requestParameters) {
        final List<String> errors = new ArrayList<String>();
        if (!requestParameters.containsKey(KEY_PARAM)) {
            errors.add(KEY_PARAM);
        }
        if (!requestParameters.containsKey(TYPE_PARAM)) {
            errors.add(TYPE_PARAM);
        }
        if (!requestParameters.containsKey(DISPLAY_PARAM)) {
            errors.add(DISPLAY_PARAM);
        }
        return errors;
    }

    /**
     * 
     * @param requestId
     * @param requestParameters
     * @return
     * @throws WebApplicationException
     */
    @SuppressWarnings("unused")
    private String getGridResults(final String requestId, final MultivaluedMap<String, String> requestParameters) throws WebApplicationException {
        final Map<String, Object> templateParameters = new HashMap<String, Object>();
        final String key = requestParameters.getFirst(KEY_PARAM);
        final String type = requestParameters.getFirst(TYPE_PARAM);
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
        final FTYPE_ENUM ftype;
        if (requestParameters.containsKey(FAIL_TYPE)) {
            if (requestParameters.getFirst(FAIL_TYPE).equals(BLOCKED_VALUE)) {
                ftype = FTYPE_ENUM.BLOCKED;
                templateParameters.put(FAIL_TYPE, ftype.toString());
            } else if (requestParameters.getFirst(FAIL_TYPE).equals(DROPPED_VALUE)) {
                ftype = FTYPE_ENUM.DROPPED;
                templateParameters.put(FAIL_TYPE, ftype.toString());
            }

        }
        if (requestParameters.containsKey(GROUP_NAME_PARAM)) {
            templateParameters.put(GROUP_NAME_KEY, requestParameters.getFirst(GROUP_NAME_PARAM));
        }
        List<Integer> timeColumnIndexes = null;
        FormattedDateTimeRange dateTimeRange;
        //Since for IMSI or MSISDN we always fetch from raw tables. No need to add aggregation offset
        if (TYPE_IMSI.equals(type) || TYPE_MSISDN.equals(type)) {
            dateTimeRange = getAndCheckFormattedDateTimeRange(requestParameters);
        } else {
            dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(requestParameters, TechPackData.completeTechPackListForMss);
        }
        final String drillType = null;
        updateTemplateParametersWithGroupDefinitionForHashId(templateParameters, requestParameters);

        final String timerange = getTimeRange(dateTimeRange);
        templateParameters.put(TIMERANGE_PARAM, timerange);
        templateParameters.put(IS_EXCULDED_TAC_OR_TACGROUP, isExcludedTacOrGroup(requestParameters));
        if (!isValidValue(requestParameters)) {
            return JSONUtils.jsonErrorInputMsg();
        }
        FormattedDateTimeRange newDateTimeRange;
        if (TYPE_IMSI.equals(type) || TYPE_MSISDN.equals(type)) {
            newDateTimeRange = dateTimeRange;
        } else {
            newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange, TechPackData.completeTechPackListForMss);
        }
        templateParameters.put(KEY_PARAM, key);
        templateParameters.put(TYPE_PARAM, type);
        templateParameters.put(INTERNAL_CAUSE_CODE_PARAM, requestParameters.getFirst(INTERNAL_CAUSE_CODE_PARAM));
        templateParameters.put(FAULT_CODE_PARAM, requestParameters.getFirst(FAULT_CODE_PARAM));
        templateParameters.put(COUNT_PARAM, getCountValue(requestParameters, MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE));
        templateParameters.put(EVENT_ID_PARAM, requestParameters.getFirst(EVENT_ID_PARAM));
        templateParameters.put(COUNT_PARAM, getCountValue(requestParameters, MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE));
        final TechPackTables techPackTables = getRawTables(type, newDateTimeRange);
        if (techPackTables.getRawTables().size() <= 0) {
            return JSONUtils.JSONEmptySuccessResult();
        }
        templateParameters.put(TECH_PACK_TABLES, techPackTables);
        if (key.equals(KEY_TYPE_ERR) || key.equals(KEY_TYPE_SUC) || key.equals(KEY_TYPE_TOTAL)) {
            final String evntID = (String) templateParameters.get(EVENT_ID_PARAM);
            timeColumnIndexes = getEventTypeSpecificTimeColumns(evntID);
        } else if (key.equals(KEY_TYPE_SUM)) {
            templateParameters.put(TIMERANGE_PARAM, timerange);
        }

        if (isMediaTypeApplicationCSV()) {
            char sign = tzOffset.charAt(0);
            String first = tzOffset.substring(1, 3);
            String last = tzOffset.substring(3, 5);
            Integer total = (Integer.parseInt(first) * 60 + Integer.parseInt(last));
            String tzOffsetQuery = sign + total.toString();
            templateParameters.put(CSV_PARAM, new Boolean(true));
            templateParameters.put(TZ_OFFSET, tzOffsetQuery);
        }

        final List<String> queries = queryConstructor.getMssEventAnalysisQuery(templateParameters);
        if (queries == null || queries.isEmpty()) {
            return JSONUtils.JSONBuildFailureError();
        }
        checkAndCreateFineAuditLogEntryForQuery(requestParameters, queries, newDateTimeRange);

        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, tzOffset, timeColumnIndexes, queries, newDateTimeRange);
            return null;
        }

        return this.dataService.getGridDataWithAppendedRows(requestId, queries,
                this.queryUtils.mapRequestParametersForHashId(requestParameters, newDateTimeRange), timeColumnIndexes, tzOffset,
                getLoadBalancingPolicy(requestParameters));
    }

    /**
     * Test purpose
     * 
     * @param queryConstructor
     */
    public void setQueryConstructor(final QueryConstructor queryConstructor) {
        this.queryConstructor = queryConstructor;
    }
}
