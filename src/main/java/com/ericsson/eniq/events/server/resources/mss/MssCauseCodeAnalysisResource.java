/**
* -----------------------------------------------------------------------
*     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
* -----------------------------------------------------------------------
*/
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
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.common.ApplicationConstants;
import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * @author egraman
 * @since 2011
 * 
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class MssCauseCodeAnalysisResource extends MssBaseResource {

    //	@EJB
    //private TechPackCXCMappingService techPackCXCMappingService;*/

    /**
     * Constructor for class
     * Initializing the look up maps - cannot have these as static, or initialized in a static block, as they
     * can be initialized from several classes
     */

    public MssCauseCodeAnalysisResource() {
        aggregationViews = new HashMap<String, AggregationTableInfo>();

        aggregationViews.put(TYPE_INTERNAL_CAUSE_CODE, new AggregationTableInfo(EVNTSRC_CC_FC,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));

        aggregationViews.put(TYPE_MSC, new AggregationTableInfo(EVNTSRC_CC_FC, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));

        aggregationViews.put(TYPE_BSC, new AggregationTableInfo(HIER3_CC_FC, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
    }

    @EJB
    private InternalCauseCodeTablesICCResource internalCauseCodeTablesICCResource;

    @EJB
    private InternalCauseCodeTablesFCResource internalCauseCodeTablesFCResource;

    /**
     * @return the internalCauseCodeTablesICCResource
     */
    @Path(INTERNAL_CAUSE_CODE_TABLE_ICC)
    public InternalCauseCodeTablesICCResource getInternalCauseCodeTablesICCResource() {
        return internalCauseCodeTablesICCResource;
    }

    /**
     * @return the internalCauseCodeTablesFCResource
     */
    @Path(INTERNAL_CAUSE_CODE_TABLE_FC)
    public InternalCauseCodeTablesFCResource getInternalCauseCodeTablesFCResource() {
        return internalCauseCodeTablesFCResource;
    }

    /**
     * taken directly from the velocity template
     * @param timerange
     * @param type
     * @return
     */
    boolean shouldQueryUseAggregationTables(final String timerange, final String type) {
        if ((FIFTEEN_MINUTES.equals(timerange) || DAY.equals(timerange)) && (!TYPE_CELL.equals(type))) {
            return true;
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax
     * .ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.ericsson.eniq.events.server.resources.BaseResource#getData(java.lang
     * .String, javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
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
     * @see
     * com.ericsson.eniq.events.server.resources.BaseResource#checkParameters(
     * javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected List<String> checkParameters(final MultivaluedMap<String, String> requestParameters) {
        final List<String> errors = new ArrayList<String>();

        if (!requestParameters.containsKey(INTERNAL_CAUSE_CODE_PARAM)) {
            if (requestParameters.containsKey(TYPE_PARAM)) {
                if (!requestParameters.containsKey(NODE_PARAM) && !requestParameters.containsKey(GROUP_NAME_PARAM)) {
                    errors.add(NODE_PARAM + " or " + GROUP_NAME_PARAM + " missing");
                }
            } else {
                errors.add(TYPE_PARAM);
            }
        }
        if (!requestParameters.containsKey(DISPLAY_PARAM)) {
            errors.add(DISPLAY_PARAM);
        }
        return errors;
    }

    /**
     * If a particular type is restricted to only certain tech pack(s) (eg BSC is
     * only applicable for the EVENT_E_MSS tech pack) then this method returns
     * only that tech pack(s) Otherwise just returns complete list of tech packs
     * 
     * @return
     */
    private List<String> getTechPacksApplicableForType() {

        final List<String> listOfApplicableTechPacks = new ArrayList<String>();
        listOfApplicableTechPacks.add(EVENT_E_MSS_TPNAME);
        return listOfApplicableTechPacks;

    }

    /**
     * add various template parameters
     * 
     * @param requestParameters
     * @param templateParameters
     * @param timerange
     * @param type
     * @param techPackTables
     */
    // TODO - Consider Refactoring to baseResources with overloaded version called
    // in each resource
    private void updateTemplateParameters(final MultivaluedMap<String, String> requestParameters,
            final Map<String, Object> templateParameters, final String timerange, final String type,
            final TechPackTables techPackTables) {
        templateParameters.put(TYPE_PARAM, type);
        templateParameters.put(TIMERANGE_PARAM, timerange);
        templateParameters.put(COUNT_PARAM,
                getCountValue(requestParameters, MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE));
        templateParameters.put(TECH_PACK_TABLES, techPackTables);
        templateParameters.put(USE_AGGREGATION_TABLES, shouldQueryUseAggregationTables(timerange, type));
    }

    /**
     * Gets the grid results.
     * 
     * @param requestId
     *          corresponds to this request for cancelling later
     * @param requestParameters
     *          the request parameters
     * @param dateTimeRange
     *          the date time range
     * @return the grid results
     * @throws WebApplicationException
     *           the parse exception
     */
    private String getGridResults(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(
                requestParameters, getTechPacksApplicableForType());

        final Map<String, Object> templateParameters = new HashMap<String, Object>();

        final String timerange = getTimeRange(dateTimeRange);
        final String key = requestParameters.getFirst(KEY_PARAM);
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
        String type = TYPE_INTERNAL_CAUSE_CODE;
        final List<Integer> timeColumnIndexes = null;

        if (requestParameters.containsKey(TYPE_PARAM)) {
            type = requestParameters.getFirst(TYPE_PARAM);
        }

        if (requestParameters.containsKey(GROUP_NAME_PARAM)) {
            templateParameters.put(GROUP_NAME_PARAM, requestParameters.get(GROUP_NAME_PARAM));
        }

        if (requestParameters.containsKey(KEY_PARAM)) {
            templateParameters.put(KEY_PARAM, key);
        }

        templateParameters.put(TIMERANGE_PARAM, timerange);
        templateParameters.put(TYPE_PARAM, type);

        final List<String> cxcLicensesForTechPack = techPackCXCMappingService.getTechPackCXCNumbers(EVENT_E_MSS_TPNAME);

        if (cxcLicensesForTechPack.isEmpty()) {
            return JSONUtils.createJSONErrorResult("MSS feature has not been installed.");
        }

        final TechPackTables techPackTables = getRawTables(type, dateTimeRange);

        if (techPackTables.shouldReportErrorAboutEmptyRawTables()) {
            return JSONUtils.JSONEmptySuccessResult();
        }
        updateTemplateParameters(requestParameters, templateParameters, timerange, type, techPackTables);
        if (shouldQueryUseAggregationTables(timerange, type)) {
            //TODO use updateTemplateParameters instead
            final TechPackTables techPackAggViews = getAggregationTables(type, timerange,
                    getTechPacksApplicableForType());
            templateParameters.put("techPackAggViews", techPackAggViews);
        }

        final String query = templateUtils.getQueryFromTemplate(
                getTemplate(ApplicationConstants.INTERNAL_CAUSE_CODE_ANALYSIS, requestParameters, null),
                templateParameters);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        checkAndCreateFineAuditLogEntryForQuery(requestParameters, query, dateTimeRange);

        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, tzOffset, timeColumnIndexes, query, dateTimeRange);
            return null;
        }

        Map<String, QueryParameter> queryParameters;
        queryParameters = this.queryUtils.mapRequestParametersForHashId(requestParameters, dateTimeRange);

        return this.dataService.getGridData(requestId, query, queryParameters, timeColumnIndexes, tzOffset,
                getLoadBalancingPolicy(requestParameters));
    }

}
