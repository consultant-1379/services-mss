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

/**
 * @author echchik
 * @since 2011
 * 
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class MssServiceResource {

    @EJB
    protected MssNetworkServiceResource mssNetworkServiceResource;

    @EJB
    protected MssTerminalServiceResource mssTerminalServiceResource;

    @EJB
    protected MssSubscriberServiceResource mssSubscriberServiceResource;

    /**
     * @return the mssNetworkServiceResource
     */
    @Path(NETWORK_SERVICES)
    public MssNetworkServiceResource getMssNetworkServiceResource() {
        return mssNetworkServiceResource;
    }

    /**
     * @return the mssTerminalServiceResource
     */
    @Path(TERMINAL_SERVICES)
    public MssTerminalServiceResource getMssTerminalServiceResource() {
        return mssTerminalServiceResource;
    }

    /**
     * Subroot-resource locator method for Mss SUBSCRIBER service Retrieves
     * representation of an instance of MssSubscriberServiceResource.
     * 
     * @return an instance of MssSubscriberServiceResource
     */
    @Path(SUBSCRIBER_SERVICES)
    public MssSubscriberServiceResource getMssSubscriberServiceResource() {
        return this.mssSubscriberServiceResource;
    }
}
