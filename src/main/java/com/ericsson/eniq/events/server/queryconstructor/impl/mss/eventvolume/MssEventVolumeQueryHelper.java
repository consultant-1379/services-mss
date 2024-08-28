package com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;
import javax.ejb.Stateless;

import com.ericsson.eniq.events.server.queryconstructor.impl.mss.eventvolume.MssEventVolumeQueryCreatorEnumConstants.MssTableType;

@Stateless
public class MssEventVolumeQueryHelper {

    @EJB
    protected MssEventVolumeQueryVoiceCreator mssEventVolumeQueryVoiceCreator;

    @EJB
    protected MssEventVolumeQueryLocServiceCreator mssEventVolumeQueryLocServiceCreator;

    @EJB
    protected MssEventVolumeQuerySMSCreator mssEventVolumeQuerySMSCreator;

    @EJB
    protected MssEventVolumeQueryImsubCreator mssEventVolumeQueryImsubCreator;
    
    /**
     *  mssTableTypes is used for control support Voice, Loc_Service or SMS in future
     */
    private static Map<MssTableType, Boolean> mssTableTypes = new HashMap<MssTableType, Boolean>();

    static {
        mssTableTypes.put(MssTableType.VOICE, true);
        mssTableTypes.put(MssTableType.SMS, true);
        mssTableTypes.put(MssTableType.LOC_SERVICE, true);
    }

    /**
     * constructQuery is used to construct query for MssEventVolumeResource
     * @param  templateParameters
     * @return query list
     */
    public List<String> constructQuery(final Map<String, Object> templateParameters) {
        final List<String> queries = new ArrayList<String>();
        String query = mssEventVolumeQueryVoiceCreator.createQuery(templateParameters, mssTableTypes);
        if (query != null && !query.isEmpty()) {
            queries.add(query);
        }
        query = mssEventVolumeQueryLocServiceCreator.createQuery(templateParameters, mssTableTypes);
        if (query != null && !query.isEmpty()) {
            queries.add(query);
        }
        
        query = mssEventVolumeQuerySMSCreator.createQuery(templateParameters, mssTableTypes);
        if (query != null && !query.isEmpty()) {
            queries.add(query);
        }
        
        query = mssEventVolumeQueryImsubCreator.createQuery(templateParameters, mssTableTypes);
        if (query != null && !query.isEmpty()) {
            queries.add(query);
        }
        return queries;
    }

    /**
     * for unittest
     * @param mssEventVolumeQueryVoiceCreator
     */
    public void setMssEventVolumeQueryVoiceCreator(final MssEventVolumeQueryVoiceCreator mssEventVolumeQueryVoiceCreator) {
        this.mssEventVolumeQueryVoiceCreator = mssEventVolumeQueryVoiceCreator;
    }

    /**
     * for unittest
     * @param mssEventVolumeQueryLocServiceCreator
     */
    public void setMssEventVolumeQueryLocServiceCreator(
            final MssEventVolumeQueryLocServiceCreator mssEventVolumeQueryLocServiceCreator) {
        this.mssEventVolumeQueryLocServiceCreator = mssEventVolumeQueryLocServiceCreator;
    }
    
    /**
     * for unittest
     * @param mssEventVolumeQuerySMSCreator
     */
    public void setMssEventVolumeQuerySMSCreator(
            final MssEventVolumeQuerySMSCreator mssEventVolumeQuerySMSCreator) {
        this.mssEventVolumeQuerySMSCreator = mssEventVolumeQuerySMSCreator;
    }

    /**
     * for unittest
     * @param mssEventVolumeQueryImsubCreator
     */
    public void setMssEventVolumeQueryImsubCreator(final MssEventVolumeQueryImsubCreator mssEventVolumeQueryImsubCreator) {
        this.mssEventVolumeQueryImsubCreator = mssEventVolumeQueryImsubCreator;
    }

}
