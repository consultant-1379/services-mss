/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.mss;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.Path;

@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class MssSubscriberServiceResource {

    @EJB
    protected MssSubsessionBIResource mssSubsessionBIResource;

    /**
     * Gets the Mss SubBI resource.
     *
     * @return the Mss SubBI resource
     */
    @Path(SUBBI)
    public MssSubsessionBIResource getSubsessionBIResource() {
        return this.mssSubsessionBIResource;
    }
}
