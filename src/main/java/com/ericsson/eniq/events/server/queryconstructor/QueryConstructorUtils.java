/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.queryconstructor;

import java.util.Map;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Singleton;

import com.ericsson.eniq.events.server.templates.exception.ResourceInitializationException;
import com.ericsson.eniq.events.server.templates.utils.TemplateUtils;

@Singleton
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class QueryConstructorUtils {

    @EJB
    TemplateUtils templateUtils;

    /**
     * Uses TemplateUtils to generate a query based on the given Template file and the parameters 
     * @param templateFileName
     * @param parameters
     * @return a query that can be run into the database, or combined with other templates to build up a query
     */

    public String getQueryFromTemplate(final String templateName, final Map<String, Object> parameters)
            throws ResourceInitializationException {
        return templateUtils.getQueryFromTemplate(templateName, parameters);
    }

    /**
     * Method "possibly" required by the SpringFramework to inject templateUtils into this class.
     * For sure this is needed for the Integration tests, but is it needed in production code.
     * @param templateUtilities
     */
    public void setTemplateUtils(final TemplateUtils templateUtilities) {
        templateUtils = templateUtilities;
    }
}
