/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.mss.piechart;

import static com.ericsson.eniq.events.server.integritytests.mss.piechart.MSSPieChartCauseCodePopulator.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.*;

import com.ericsson.eniq.events.server.test.queryresults.PieChartCauseCodeAnalysisMSSResult;
import com.ericsson.eniq.events.server.test.queryresults.PieChartCauseCodeListResult;
import com.ericsson.eniq.events.server.test.queryresults.mss.MSSPieChartSubCauseCodeAnalysisResult;

/**
 * @author eavidat
 * @since 2011
 *
 */
public class MSSPieChartCauseCodeDataValidator {

    private final static Map<Integer, String> internalCauseCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> faultCodeMapping = new HashMap<Integer, String>();

    static {
        // Populate SGEH cause codes/descriptions
        internalCauseCodeMapping.put(internal_causeCode_1, internal_causeCode_1_desc);
        internalCauseCodeMapping.put(internal_causeCode_2, internal_causeCode_2_desc);
        internalCauseCodeMapping.put(internal_causeCode_3, internal_causeCode_3_desc);

        // Populate SGEH sub cause codes/descriptions
        faultCodeMapping.put(faultCode_1, faultCode_1_desc);
        faultCodeMapping.put(faultCode_2, faultCode_2_desc);
    }

    public void makeAssertionsCCList(final List<PieChartCauseCodeListResult> summaryResult) {
        assertThat(summaryResult.size(), is(2));

        final PieChartCauseCodeListResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getCauseCodeID(), is(Integer.toString(internal_causeCode_1)));
        assertThat(firstResult.getCauseCode(), is(internalCauseCodeMapping.get(internal_causeCode_1)));

        final PieChartCauseCodeListResult secondResult = summaryResult.get(1);
        assertThat(secondResult.getCauseCodeID(), is(Integer.toString(internal_causeCode_2)));
        assertThat(secondResult.getCauseCode(), is(internalCauseCodeMapping.get(internal_causeCode_2)));
    }

    public void makeAssertionsCCAnaylsis(final List<PieChartCauseCodeAnalysisMSSResult> summaryResult) {
        assertThat(summaryResult.size(), is(2));

        final PieChartCauseCodeAnalysisMSSResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getCauseCodeID(), is(Integer.toString(internal_causeCode_1)));
        assertThat(firstResult.getCauseCode(), is(internalCauseCodeMapping.get(internal_causeCode_1)));
        assertThat(firstResult.getNumberOfErrors(), is(4));
        assertThat(firstResult.getNumberOfOccurences(), is(3));

        final PieChartCauseCodeAnalysisMSSResult secondResult = summaryResult.get(1);
        assertThat(secondResult.getCauseCodeID(), is(Integer.toString(internal_causeCode_2)));
        assertThat(secondResult.getCauseCode(), is(internalCauseCodeMapping.get(internal_causeCode_2)));
        assertThat(secondResult.getNumberOfErrors(), is(2));
        assertThat(secondResult.getNumberOfOccurences(), is(1));
    }

    public void makeAssertionsSCCAnaylsis(final List<MSSPieChartSubCauseCodeAnalysisResult> summaryResult) {
        assertThat(summaryResult.size(), is(1));

        final MSSPieChartSubCauseCodeAnalysisResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getCauseCodeID(), is(internal_causeCode_1));
        assertThat(firstResult.getSubCauseCodeID(), is(faultCode_1));
        assertThat(firstResult.getSubCauseCode(), is(faultCodeMapping.get(faultCode_1)));
        assertThat(firstResult.getNumberOfErrors(), is(4));
        assertThat(firstResult.getNumberOfOccurences(), is(3));
    }
}