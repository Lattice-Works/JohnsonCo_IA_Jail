package com.openlattice.integrations.JohnsonCo_IA_Jail;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.openlattice.client.RetrofitFactory.Environment;
import com.openlattice.shuttle.Flight;
import com.dataloom.mappers.ObjectMappers;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;

import static com.openlattice.shuttle.util.Parsers.getAsString;

public class JohnsonCoJailBookings {
    private static final Logger logger = LoggerFactory
            .getLogger( JohnsonCoJailBookings.class );

    private static final Environment environment = Environment.LOCAL;

    private static final DateTimeHelper dHelper = new DateTimeHelper( TimeZones.America_Chicago,
            "MM/dd/yyyy" );

    public static void main( String[] args ) throws InterruptedException {
        if ( args.length < 3 ) {
            System.out.println( "expected: <path> <jwtToken>" );
            return;
        }
        final String jailPath = args[ 1 ];
        final String jwtToken = args[ 2 ];

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        SimplePayload payload = new SimplePayload( jailPath );

        //formatter:off
        Flight jailflight = Flight.newFlight()
                .createEntities()

                //PEOPLE - 2 entity sets, inmates and officers
                .addEntity( "inmateperson" )
                .to( "JCInmates" )
                    .addProperty( "justice.xref", "MNI_No" )
                    .addProperty( "nc.SubjectIdentification", "MNI_No" )
                    .addProperty( "nc.PersonGivenName" )
                        .value( row -> getFirstName( row.getAs( "JName" ) ) ).ok()
                    .addProperty( "nc.PersonMiddleName" )
                        .value( row -> getMiddleName( row.getAs( "JName" ) ) ).ok()
                    .addProperty( "nc.PersonSurName" )
                        .value( row -> getLastName( row.getAs( "JName" ) ) ).ok()
                    .addProperty( "nc.PersonBirthDate" )
                        .value( row -> dHelper.parseDate( row.getAs( "DOB" ) ) ).ok()
                    .addProperty( "nc.PersonSex", "Sex" )
                    .addProperty( "nc.PersonRace" )
                        .value( row -> standardRace( row.getAs( "Race" ) ) ).ok()
                .endEntity()
                .addEntity( "Aofficerperson" )
                    .to( "JCOfficers" )
                    .addProperty( "nc.PersonGivenName" )
                        .value( row -> getFirstName( row.getAs( "AO" ) ) ).ok()
                    .addProperty( "nc.PersonSurName" )
                        .value( row -> getLastName( row.getAs( "AO" ) ) ).ok()
                    .addProperty( "ncSubjectIdentification" )
                        .value( JohnsonCoJailBookings::getAOfficerID ).ok()      //cannot just use AO_ID because the integration will collapse everyone with a blank ID into 1 person.
                .endEntity()
                .addEntity( "Rofficerperson" )
                    .to( "JCOfficers" )
                    .addProperty( "nc.PersonGivenName" )
                        .value( row -> getFirstName( row.getAs( "Rel_Officer" ) ) ).ok()
                    .addProperty( "nc.PersonSurName" )
                        .value( row -> getLastName( row.getAs( "Rel_Officer" ) ) ).ok()
                    .addProperty( "nc.SubjectIdentification", "Rel_Officer_ID" )
                .endEntity()
                .addEntity( "Sofficerperson" )
                    .to( "JCOfficers" )
                    .addProperty( "nc.PersonGivenName" )
                        .value( row -> getFirstName( row.getAs( "Search_Officer" ) ) ).ok()
                    .addProperty( "nc.PersonSurName" )
                        .value( row -> getLastName( row.getAs( "Search_Officer" ) ) ).ok()
                    .addProperty( "nc.SubjectIdentification", "Search_Officer_ID" )
                .endEntity()
                .addEntity( "Tofficerperson" )
                    .to( "JCOfficers" )
                    .addProperty( "nc.PersonGivenName" )
                        .value( row -> getFirstName( row.getAs( "Transp_Officer" ) ) ).ok()
                    .addProperty( "nc.PersonSurName" )
                        .value( row -> getLastName( row.getAs( "Transp_Officer" ) ) ).ok()
                    .addProperty( "nc.SubjectIdentification" )
                        .value( JohnsonCoJailBookings::getTOfficerID ).ok()
                .endEntity()

                //OTHER PERSONAL INFO - 2 entity sets, for inamtes and officers
                .addEntity( "JIinfo" )
                    .to( "JCJIPersonInfo" )
                    .addProperty( "nc.SubjectIdentification", "MNI_No" )
                    .addProperty( "im.PersonNickName", "Alias" )
                    .addProperty( "nc.PersonWeightMeasure" ).value( row -> Parsers.parseInt( row.getAs( "OWeight" ) ) ).ok()
                    .addProperty( "nc.PersonHeightMeasure" ).value( row -> Parsers.parseInt( row.getAs( "OHeight" ) ) ).ok()
                    .addProperty( "nc.PersonEyeColorText", "OEyes" )
                    .addProperty( "nc.PersonHairColorText", "OHair" )
                    .addProperty( "person.stateidnumber", "MNI_No" )
                    .addProperty( "criminaljustice.cautioninformation", "Caution" )
                .endEntity()
                .addEntity( "Aofficers" )
                    .to( "JCOfficerInfo" )
                    .addProperty( "publicsafety.personnelid" )
                        .value( JohnsonCoJailBookings::getAOfficerID ).ok()      //cannot just use AO_ID because the integration will collapse everyone with a blank ID into 1 person.
                    .addProperty( "publicsafety.agencyname", "Arrest_Agency" )
                .endEntity()
                .addEntity( "Rofficers" )
                    .to( "JCOfficerInfo" )
                    .addProperty( "publicsafety.personnelid","Rel_Officer_ID" ) //checked, no blanks where there is a rel officer name
                .endEntity()
                .addEntity( "Sofficers" )
                    .to( "JCOfficerInfo" )
                    .addProperty( "publicsafety.personnelid", "Search_Officer_ID" ) //checked, no blanks where there is a search officer name
                .endEntity()
                .addEntity( "Tofficers" )
                    .to( "JCOfficerInfo" )
                    .addProperty( "publicsafety.personnelid" )
                         .value( JohnsonCoJailBookings::getTOfficerID ).ok()      //cannot just use AO_ID because the integration will collapse everyone with a blank ID into 1 person.
                    .addProperty( "publicsafety.agencyname", "Transp_Agency" )
                .endEntity()

                //other entity sets
                .addEntity( "jailstay" )
                    .to( "JCJailRecord" )
                    .addProperty( "criminaljustice.jailrecordid", "Jail_ID" )
                    .addProperty( "criminaljustice.inmateid", "MNI_No" )
                    .addProperty( "ol.datetimestart")
                        .value( row -> dHelper.parseDate( row.getAs( "Date_In" ) ) ).ok()
                    .addProperty( "ol.datetime_release" )
                        .value( row -> dHelper.parseDate( row.getAs( "Date_Out" ) ) ).ok()
                    .addProperty( "ol.date_projectedrelease" )
                        .value( row -> dHelper.parseDate( row.getAs( "Est_Rel_Date" ) ) ).ok()
                    .addProperty( "criminaljustice.treatasadult", "Adult_Juv_Waive" )
                    .addProperty( "publicsafety.JuvHoldAuth", "Juv_Hold_Auth" )
                    .addProperty( "crimianljustice.bailingperson", "Person_Post" )
                    .addProperty( "criminaljustice.committingauthority", "Comit_Auth" )
                    .addProperty( "criminaljustice.releasetype", "How_Rel" )
                    .addProperty( "criminaljustice.releasetofacility", "Released_To" )
                    .addProperty( "criminaljustice.releasenotes", "ReleaseNotes" )
                    .addProperty( "criminaljustice.heldat", "Held_At" )
                    .addProperty( "ol.reasoncode", "ReasonCode" )
                    .addProperty( "publicsafety.ssa", "SSA" )
                    .addProperty( "publicsafety.ssaconviction" )
                        .value( row -> dHelper.parseDate( row.getAs( "SSA_Conviction" ) ) ).ok()
                    .addProperty( "place.originatingagencyidentifier", "ORI" )
                    .addProperty( "general.status", "Status" )
                    .addProperty( "event.comments", "Remarks" )
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( "bookedin" )
                    .to( "JCBooking" )
                    .fromEntity( "inmateperson" )
                    .toEntity( "jailstay" )
                    .entityIdGenerator( row ->  row.get( "BookedID" ) + row.get( "Jail_ID" ))
                    .addProperty( "general.stringid", "BookedID" )
                    .addProperty( "person.ageatevent", "Age" )
                    .addProperty( "j.intoxicationlevel" ).value( row -> Parsers.parseDouble( "Intox" ) ).ok()
                    .addProperty( "publicsafety.portablebreathtest" ).value( row -> Parsers.parseDouble( "PBT" ) ).ok()
                    .addProperty( "event.comments", "Remarks" )
                .endAssociation()
                .addAssociation( "is1" )
                    .to( "JCis" )
                    .fromEntity( "inmateperson" )
                    .toEntity( "JIinfo" )
                    .addProperty( "general.stringid", "Jail_ID" )
                    .addProperty( "ol.datetimestart" )
                        .value( row -> dHelper.parseDate( row.getAs( "Date_In" )) ).ok()
                    .addProperty( "incident.enddatetime" )
                        .value( row -> dHelper.parseDate( row.getAs( "Date_Out" )) ).ok()
                .endAssociation()
                .addAssociation( "is2" )
                    .to( "JCis" )
                    .fromEntity( "Aofficerperson" )
                    .toEntity( "Aofficers" )
                    .addProperty( "general.stringid" )
                        .value( JohnsonCoJailBookings::getAOfficerID ).ok()
                .endAssociation()
                .addAssociation( "is3" )
                    .to( "JCis" )
                    .fromEntity( "Rofficerperson" )
                    .to( "Rofficers" )
                    .addProperty( "general.stringid", "Rel_Officer_ID" )
                .endAssociation()
                .addAssociation( "is4" )
                    .to( "JCis" )
                    .fromEntity( "Sofficerperson" )
                    .toEntity( "Sofficers" )
                    .addProperty( "general.stringid", "Search_Officer_ID" )
                .endAssociation()
                .addAssociation( "is5" )
                    .to( "JCis" )
                    .fromEntity( "Tofficerperson" )
                    .toEntity( "Tofficers" )
                    .addProperty( "general.stringid" )
                        .value( JohnsonCoJailBookings::getTOfficerID ).ok()
                .endAssociation()
                .addAssociation( "arrestedby" )
                    .to( "JCArrestedBy" )
                    .fromEntity( "inmateperson" )
                    .toEntity( "Aofficers" )
                    .addProperty( "general.StringID" )
                        .value( row -> row.getAs( "MNI_No" ) + getAOfficerID( row ) ).ok()
                    .addProperty( "publicsafety.ArrestID", "Arrest_No" )
                    .addProperty( "ol.arrestdatetime" )
                        .value( row -> dHelper.parseDate( row.getAs( "Arrest_Date" ) ) ).ok()
                .endAssociation()
                .addAssociation( "searchedby" )
                    .to( "JCSearchedby" )
                    .fromEntity( "inmateperson" )
                    .toEntity( "Sofficers" )
                    .addProperty( "general.StringID" )
                        .value( row -> row.getAs( "Search_Officer_ID" ) + row.getAs( "MNI_No" ) ).ok()
                .endAssociation()
                .addAssociation( "releasedby" )
                    .to( "JCReleasedby" )
                    .fromEntity( "inmateperson" )
                    .toEntity( "Rofficers" )
                    .addProperty( "general.StringID" )
                        .value( row -> row.getAs( "Rel_Officer_ID" ) + row.getAs( "MNI_No" ) ).ok()
                .endAssociation()
                .addAssociation( "transportedby" )
                    .to( "JCTransportedby" )
                    .fromEntity( "inmateperson" )
                    .toEntity( "Tofficers" )
                    .addProperty( "general.StringID" )
                        .value( row -> getTOfficerID( row ) + row.getAs( "MNI_No" ) ).ok()
                .endAssociation()

                .endAssociations()
                .done();


        //formatter:on


        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Payload> flights = new HashMap<>( 1 );
        flights.put( jailflight, payload );

        shuttle.launchPayloadFlight( flights );
    }

    public static String getFirstName( Object obj ) {
        if ( obj != null ) {
            String name = obj.toString();
            String[] names = name.split( "," );
            if ( names.length > 1 ) {
                if ( names[ 1 ].length() > 1 ) {
                    String fix = names[ 1 ].trim();
                    String[] newnames = fix.split( " " );
                    if ( newnames.length > 1 ) {
                        return newnames[ 0 ].trim();
                    }
                    return names[ 1 ].trim();
                }
                return null;
            }
            return null;
        }
        return null;
    }

    public static String getLastName( Object obj ) {
        if ( obj != null ) {
            String name = obj.toString();
            if ( !name.equals( "" ) ) {
                String[] names = name.split( "," );
                if ( names.length > 0 ) {
                    return names[ 0 ].trim();
                }
                return null;
            }
            return null;
        }
        return null;
    }

    public static String getMiddleName( Object obj ) {
        if ( obj != null ) {
            String name = obj.toString();
            String[] names = name.split( "," );
            if ( names.length > 1 ) {
                if ( names[ 1 ].length() > 1 ) {
                    String fix = names[ 1 ].trim();
                    String[] newnames = fix.split( " " );
                    if ( newnames.length > 1 ) {
                        return newnames[ 1 ].trim();
                    }
                    return null;
                }
                return null;
            }
            return null;
        }
        return null;
    }

    public static String standardRace( Object obj ) {
        String sr = getAsString( obj );

        if ( sr != null ) {
            if ( sr.equals( "A" ) ) {return "asian"; }
            if ( sr.equals( "W" ) ) { return "white"; }
            if ( sr.equals( "B" ) ) { return "black"; }
            if ( sr.equals( "I" ) ) { return "amindian"; }
            if ( sr.equals( "U" ) || sr.equals( "" ) ) { return null; }
        }
        return null;
    }

    public static String getAOfficerID( Row row ) {
        String id = row.getAs( "AO_ID" );
        if ( StringUtils.isNotBlank( id ) ) {
            return id;
        }
        return row.getAs( "ArrestOfficerID" );
    }

    public static String getTOfficerID( Row row ) {
        String id = row.getAs( "Transp_Officer_ID" );
        if ( StringUtils.isNotBlank( id ) ) {
            return id;
        }

        String agency = row.getAs( "Transp_Agency" );
            if (StringUtils.isNotBlank( agency )) {

            String name = row.getAs( "Transp_Officer" );
            if (StringUtils.isNotBlank( name )) {
                StringBuilder concat = new StringBuilder( agency );
                concat.append( name );
                return concat.toString();
            }
                id = UUID.randomUUID().toString();
                return id;
        }
          id = UUID.randomUUID().toString();
            return id;
    }

}
