package com.openlattice.integrations.JohnsonCo_IA_Jail;

import java.io.File;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.io.IOException;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.openlattice.shuttle.config.IntegrationConfig;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.JavaDateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.JdbcPayload;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.util.Parsers;
import com.zaxxer.hikari.HikariDataSource;
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

    private static final Environment environment = Environment.PRODUCTION;

    private static final JavaDateTimeHelper dHelper = new JavaDateTimeHelper( TimeZones.America_Chicago,
            "MM/dd/yyyy", "dd-MMM-YY", "M/d/yyyy", "yyyy-MM-dd" );

    public static void main( String[] args ) throws InterruptedException, IOException  {
//        if ( args.length < 3 ) {
//            System.out.println( "expected: <path> <jwtToken>" );
//            return;
//        }
//        final String username = args[ 0 ];                                            //for running on Atlas
//        final String password = args[ 1 ];                                            //for running on Atlas
//        final String jwtToken = MissionControl.getIdToken( username, password );      //for running on Atlas
//        final String integrationFile = args[ 2 ];                                     //for running on Atlas
        final String jwtToken = args[ 0 ];                                         //for running locally
        final String integrationFile = args[ 1 ];                                  //for running locally

        HikariDataSource hds =
                ObjectMappers.getYamlMapper()
                        .readValue( new File( integrationFile ), IntegrationConfig.class )
                        .getHikariDatasource( "jciowa" );

        Payload payload = new JdbcPayload( hds,
                "SELECT jc_jail_records.\"Arrest_No\", jc_jail_records.\"MNI_No\", jc_jail_records.\"JName\", jc_jail_records.\"Alias\", "
                        + "jc_jail_records.\"Date_In\", jc_jail_records.\"Date_Out\", jc_jail_records.\"AO\", jc_jail_records.\"AO_ID\", "
                        + "jc_jail_records.\"DOB\",  jc_jail_records.\"Age\", jc_jail_records.\"How_Rel\", jc_jail_records.\"Rel_Officer\", "
                        + "jc_jail_records.\"Rel_Officer_ID\", jc_jail_records.\"Caution\",  jc_jail_records.\"Search_Officer\", "
                        + "jc_jail_records.\"Search_Officer_ID\", jc_jail_records.\"Est_Rel_Date\", jc_jail_records.\"Released_To\", "
                        + "jc_jail_records.\"Remarks\", jc_jail_records.\"Comit_Auth\", jc_jail_records.\"Held_At\", jc_jail_records.\"Adult_Juv_Waive\", "
                        + "jc_jail_records.\"Juv_Hold_Auth\", jc_jail_records.\"Person_Post\", jc_jail_records.\"Arrest_Agency\", jc_jail_records.\"Sex\", "
                        + "jc_jail_records.\"Race\", jc_jail_records.\"SSA\", jc_jail_records.\"SSA_Conviction\", jc_jail_records.\"SSA_Status\", "
                        + "jc_jail_records.\"ORI\", jc_jail_records.\"Status\", jc_jail_records.\"ReasonCode\", jc_jail_records.\"Arrest_Date\", "
                        + "jc_jail_records.\"OHeight\", jc_jail_records.\"OWeight\", jc_jail_records.\"OEyes\", jc_jail_records.\"OHair\", "
                        + "jc_jail_records.\"Transp_Agency\", jc_jail_records.\"Transp_Officer\", jc_jail_records.\"Transp_Officer_ID\", "
                        + "jc_jail_records.\"BookedID\", jc_jail_records.\"PBT\", jc_jail_records.\"Intox\", jc_jail_records.\"ReleaseNotes\", "
                        + "jc_jail_records.\"JailRecordId\", jc_jail_records.\"ArrestOfficerID\", jc_jail_record_offense.\"Charge\", "
                        + "jc_jail_record_offense.\"Court\", jc_jail_record_offense.\"Bond\",jc_jail_record_offense.\"State\", jc_jail_record_offense.\"Local\", "
                        + "jc_jail_record_offense.\"Off_Date\", jc_jail_record_offense.\"TSrvdDays\",jc_jail_record_offense.\"TSrvdHrs\", "
                        + "jc_jail_record_offense.\"TSrvdMins\", jc_jail_record_offense.\"Start_Date\", jc_jail_record_offense.\"Release_Date\","
                        + "jc_jail_record_offense.\"Arresting_Agency\", jc_jail_record_offense.\"Charging_Agency\", jc_jail_record_offense.\"How_Rel2\", "
                        + "jc_jail_record_offense.\"Concurrent\",jc_jail_record_offense.\"Exp_Release_Date\", jc_jail_record_offense.\"Probation\", "
                        + "jc_jail_record_offense.\"NoCounts\", jc_jail_record_offense.\"SentenceDays\",jc_jail_record_offense.\"SentenceHrs\", "
                        + "jc_jail_record_offense.\"GTDays\", jc_jail_record_offense.\"GTHrs\", jc_jail_record_offense.\"GTMins\",jc_jail_record_offense.\"GTPct\", "
                        + "jc_jail_record_offense.\"Alt_Start_Date\", jc_jail_record_offense.\"ConsecWith\", jc_jail_record_offense.\"Notes\","
                        + "jc_jail_record_offense.\"ReasonHeld\", jc_jail_record_offense.\"SexOff\", jc_jail_record_offense.\"Severity\", jc_jail_record_offense.\"NCIC\", "
                        + "jc_jail_record_offense.\"EntryDate\"   from jc_jail_records FULL OUTER JOIN jc_jail_record_offense ON "
                        + "jc_jail_records.\"Jail_ID\" = jc_jail_record_offense.\"Jail_ID\" ");

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        //        final String jailPath = args[ 0 ];
//        SimplePayload payload = new SimplePayload( jailPath );

        //formatter:off
        Flight jailflight = Flight.newFlight()
                .createEntities()

                //PEOPLE - 2 entity sets, inmates and officers
                .addEntity( "inmateperson" )
//                .to( "JCInmate" )
                .to( "IowaCityPeople2" )   //temporary hack to integrate jail people into CAD people dataset
                .useCurrentSync()
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
                    .addProperty( "nc.SubjectIdentification" )
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
                    .useCurrentSync()
                    .addProperty( "criminaljustice.personid", "MNI_No" )
                    .addProperty( "im.PersonNickName", "Alias" )
                    .addProperty( "nc.PersonWeightMeasure" ).value(  row -> getInt( row.getAs("OWeight" )) ).ok()
                    .addProperty( "nc.PersonHeightMeasure" ).value( row -> getInt( row.getAs( "OHeight" ) ) ).ok()
                    .addProperty( "nc.PersonEyeColorText", "OEyes" )
                    .addProperty( "nc.PersonHairColorText", "OHair" )
                    .addProperty( "person.stateidnumber", "MNI_No" )
                    .addProperty( "criminaljustice.cautioninformation", "Caution" )
                    .addProperty( "j.SentenceRegisterSexOffenderIndicator", "SexOff" )
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
                    .to( "JCJailStay" )
                    .addProperty( "criminaljustice.jailrecordid")
                        .value( JohnsonCoJailBookings::getJailstayId ).ok()
//                    .addProperty( "criminaljustice.inmateid", "MNI_No" )
                    .addProperty( "ol.datetimestart")
                        .value( JohnsonCoJailBookings::getDateTimeStart).ok()
                    .addProperty( "publicsafety.AlternateStartDate" )
                        .value( row -> dHelper.parseDateAsDateTime( row.getAs( "Alt_Start_Date" ) ) ).ok()
                    .addProperty( "ol.datetime_release" )
                        .value( JohnsonCoJailBookings::getDateRelease).ok()
                    .addProperty( "ol.date_projectedrelease" )
                        .value( JohnsonCoJailBookings::getDateProjRelease ).ok()
                    .addProperty( "criminaljustice.treatasadult", "Adult_Juv_Waive" )
                    .addProperty( "publicsafety.JuvHoldAuth", "Juv_Hold_Auth" )
                    .addProperty( "criminaljustice.bailingperson", "Person_Post" )
                    .addProperty( "criminaljustice.committingauthority", "Comit_Auth" )
                    .addProperty( "criminaljustice.releasetype")
                        .value( JohnsonCoJailBookings::getReleaseType ).ok()
                    .addProperty( "criminaljustice.releasetofacility", "Released_To" )
                    .addProperty( "criminaljustice.releasenotes", "ReleaseNotes" )
                    .addProperty( "ol.reasoncode", "ReasonCode" )
                    .addProperty( "publicsafety.ssa", "SSA" )
                    .addProperty( "publicsafety.ssaconviction" )
                        .value( row -> dHelper.parseDateAsDateTime( row.getAs( "SSA_Conviction" ) ) ).ok()
                    .addProperty( "place.OriginatingAgencyIdentifier", "ORI" )
                    .addProperty( "general.status", "Status" )
                    .addProperty( "event.comments", "Remarks" )
                    .addProperty( "criminaljustice.arrestagency", "Arresting_Agency" )
                    .addProperty( "criminaljustice.bailbondamount", "Bond" )
                    .addProperty( "criminaljustice.timeserveddays" )
                        .value( row -> getInt( row.getAs( "TSrvdDays" ) ) ).ok()
                    .addProperty( "ol.timeservedhours", "TSrvdHrs" )
                    .addProperty( "ol.timeservedminutes", "TSrvdMins" )
                    .addProperty( "publicsafety.ReasonHeld", "ReasonHeld" )
                    .addProperty( "publicsafety.GoodTimeDays" )
                        .value( row -> getInt( row.getAs( "GTDays" ) ) ).ok()
                    .addProperty( "publicsafety.GoodTimeHours" )
                        .value( row -> getInt( row.getAs( "GTHrs" ) ) ).ok()
                    .addProperty( "publicsafety.GoodTimeMinutes" )
                        .value( row -> getInt( row.getAs( "GTMins" ) ) ).ok()
                    .addProperty( "publicsafety.GoodTimePCT" )
                        .value( row -> getInt( row.getAs( "GTPct" ) ) ).ok()
                .endEntity()
                .addEntity( "sentence" )                           //ADD SOME OFFENSE INFO? O RESULTS IN S?
                    .to( "JCSentences" )
                    .addProperty( "publicsafety.SentenceTermDays" )
                        .value( row -> getInt( row.getAs( "SentenceDays" ) ) ).ok()
                    .addProperty( "publicsafety.SentenceTermHours" )
                        .value( row -> getInt( row.getAs( "SentenceHrs" ) ) ).ok()
                    .addProperty( "publicsafety.Concurrent", "Concurrent" )
                    .addProperty( "j.SentenceModificationProbationIndicator" )
                        .value( row -> getInt( row.getAs( "Probation" ) ) ).ok()
                    .addProperty( "publicsafety.ConsecWith" )
                        .value( row -> getInt( row.getAs( "ConsecWith" ) ) ).ok()
                    .addProperty( "publicsafety.AlternateStartDate" )
                        .value( row -> dHelper.parseDateAsDateTime( row.getAs( "Alt_Start_Date" ) ) ).ok()
                .endEntity()
                .addEntity( "offense" )
                    .to( "JCOffenses" )
                    .addProperty( "criminaljustice.offenseid", "State" )     //using the state Charge code as unique ID.
                    .addProperty( "j.ArrestCharge", "Charge" )
                    .addProperty( "criminaljustice.localstatute", "Local" )
                    .addProperty( "ol.statestatute", "State" )
                    .addProperty( "incident.startdatetime" )
                        .value( row -> dHelper.parseDateAsDateTime( row.getAs( "Off_Date" )) ).ok()
                    .addProperty( "criminaljustice.ncic", "NCIC" )
                    .addProperty( "event.comments", "Notes" )
                .endEntity()
                .addEntity( "court" )
                    .to( "JCCourt" )
                    .entityIdGenerator( row ->  row.get( "Court" ))
                    .addProperty( "location.name", "Court" )
                .endEntity()
                .addEntity( "facility" )
                    .to("JCFacility")
                    .addProperty( "general.stringid")
                        .value( JohnsonCoJailBookings::getFacility ).ok()
                    .addProperty( "location.name", "Held_At" )
                    .addProperty( "ol.type" ).value( JohnsonCoJailBookings::getType ).ok()
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( "bookedin" )
                    .to( "JCBooking" )
                    .fromEntity( "inmateperson" )
                    .toEntity( "jailstay" )
                    .entityIdGenerator( row ->  row.get( "BookedID" ) + row.get( "Jail_ID" ) + row.get( "MNI_No" ))
                    .addProperty( "general.stringid", "BookedID" )
                    .addProperty( "person.ageatevent", "Age" )
                    .addProperty( "j.intoxicationlevel" ).value( row -> Parsers.parseDouble( row.getAs( "Intox" )) ).ok()
                    .addProperty( "publicsafety.portablebreathtest" ).value( row -> Parsers.parseDouble( "PBT" ) ).ok()
                    .addProperty( "event.comments", "Remarks" )
                .endAssociation()
                .addAssociation( "is1" )
                    .to( "JCis" )
                    .fromEntity( "inmateperson" )
                    .toEntity( "JIinfo" )
                    .addProperty( "general.stringid", "MNI_No" )
                    .addProperty( "ol.datetimestart" )
                        .value( row -> dHelper.parseDateAsDateTime( row.getAs( "Date_In" )) ).ok()
                    .addProperty( "incident.enddatetime" )
                        .value( row -> dHelper.parseDateAsDateTime( row.getAs( "Date_Out" )) ).ok()
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
                    .toEntity( "Rofficers" )
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
                        .value( row -> dHelper.parseDateAsDateTime( row.getAs( "Arrest_Date" ) ) ).ok()
                .endAssociation()
                .addAssociation( "searchedby" )
                    .to( "JCSearchedby" )
                    .fromEntity( "inmateperson" )
                    .toEntity( "Sofficers" )
                    .addProperty( "general.StringID" )
                        .value( row -> {
                            return Parsers.getAsString(row.getAs( "Search_Officer_ID" )) +
                                    Parsers.getAsString( row.getAs( "MNI_No" ));
                        } ).ok()
                .endAssociation()
                .addAssociation( "releasedby" )
                    .to( "JCReleasedby" )
                    .fromEntity( "inmateperson" )
                    .toEntity( "Rofficers" )
                    .addProperty( "general.StringID" )
                        .value( row -> {
                            return Parsers.getAsString(row.getAs( "Rel_Officer_ID" )) +
                                    Parsers.getAsString( row.getAs( "MNI_No" ));
                        } ).ok()
                .endAssociation()
                .addAssociation( "transportedby" )
                    .to( "JCTransportedby" )
                    .fromEntity( "inmateperson" )
                    .toEntity( "Tofficers" )
                    .addProperty( "general.StringID" )
                        .value( row -> getTOfficerID( row ) + row.getAs( "MNI_No" ) ).ok()
                .endAssociation()
                .addAssociation( "chargedwith" )
                    .to( "JCChargedWith" )
                    .fromEntity( "inmateperson" )
                    .toEntity( "offense" )
                    .addProperty( "general.stringid" )
                        .value( row -> Parsers.getAsString( row.getAs( "MNI_No" )  + Parsers.getAsString( row.getAs( "Charge" ) )) ).ok()
                    .addProperty( "ol.numberofcounts" )
                        .value( row -> getInt( row.getAs( "NoCounts" ) ) ).ok()
                    .addProperty( "event.ChargeLevelState", "Severity" )
                    .addProperty( "event.ChargeLevel" ).value( JohnsonCoJailBookings::chargeLevel ).ok()
                    .addProperty( "publicsafety.agencyname", "Charging_Agency" )
                .endAssociation()
                .addAssociation( "resultsin1" )
                    .to( "JCResultsIn" )
                    .fromEntity( "sentence" )
                    .toEntity( "jailstay" )
                    .addProperty( "general.stringid")
                .value( row -> Parsers.getAsString( row.getAs( "SentenceDays" ) ) + Parsers.getAsString( row.getAs( "SentenceHrs" ) ) +
                        row.getAs( "Concurrent" ) + Parsers.getAsString( row.getAs( "Probation" ) ) + Parsers.getAsString( row.getAs( "ConsecWith" ) ) +
                        dHelper.parseDateAsDateTime( row.getAs( "Alt_Start_Date" ) )).ok()
                .endAssociation()
                .addAssociation( "resultsin2" )
                    .to( "JCResultsIn" )
                    .fromEntity( "offense" )
                    .toEntity( "sentence" )
                    .addProperty( "general.stringid", "State" )
                .endAssociation()
                .addAssociation( "occurredat1" )
                    .to( "JCOccurredAt" )
                    .fromEntity( "sentence" )
                    .toEntity( "court" )
                    .addProperty( "general.stringid", "Court" )
                .endAssociation()
                .addAssociation( "occurredat2" )
                    .to( "JCOccurredAt")
                    .fromEntity( "jailstay" )
                    .toEntity( "facility" )
                    .addProperty( "general.stringid")
                        .value( JohnsonCoJailBookings::getFacility ).ok()
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

    public static List<OffsetDateTime> getDateTimeStart( Row row ) {
        OffsetDateTime dateIn = dHelper.parseDateAsDateTime( row.getAs( "Date_In" ) );
        OffsetDateTime startDate = dHelper.parseDateAsDateTime( row.getAs( "Start_Date" ) );
        List<OffsetDateTime> result = Lists.newArrayList( dateIn, startDate );
        result.removeIf( val -> val == null );
        return result;
    }

    public static List<LocalDate> getDateRelease( Row row ) {
        LocalDate dateOut = dHelper.parseDate( row.getAs( "Date_Out" ) );
        LocalDate relDate = dHelper.parseDate( row.getAs( "Release_Date" ) );
        List<LocalDate> result = Lists.newArrayList( dateOut, relDate );
        result.removeIf( val -> val == null );
        return result;
    }

    public static List<LocalDate> getDateProjRelease( Row row ) {
        LocalDate relDate1 = dHelper.parseDate( row.getAs( "Est_Rel_Date" ) );
        LocalDate relDate2 = dHelper.parseDate( row.getAs( "Exp_Release_Date" ) );
        List<LocalDate> result = Lists.newArrayList( relDate1, relDate2 );
        result.removeIf( val -> val == null );
        return result;
    }

    public static List<String> getReleaseType( Row row ) {
        String howRel1 =  row.getAs( "How_Rel" ) ;
        String howRel2 =  row.getAs( "How_Rel2" ) ;
        List<String> result = Lists.newArrayList( howRel1, howRel2 );
        result.removeIf( val -> val == null );
        return result;
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

    public static List<String> getJailstayId( Row row ) {
        String jailid =  row.getAs( "Jail_ID" ) ;
        String recordid =  row.getAs( "JailRecordId" ) ;
        List<String> result = Lists.newArrayList( jailid, recordid );
        result.removeIf( val -> val == null );
        return result;
    }

    public static String chargeLevel (Row row ) {
        String severity = row.getAs( "Severity" );
        if (StringUtils.isNotBlank( severity )) {
            if ( severity.endsWith( "M" ) | severity.endsWith( "MS" )) { return "Misdemeanor"; }
            else if ( severity.endsWith( "F" ) | severity.startsWith( "FEL" )) { return "Felony"; }
            else return null;
        }
        return null;
    }

    public static String getFacility (Row row) {
        String input = row.getAs( "Held_At" );
        if (StringUtils.isNotBlank( input )) {
            return input;
        }
        return null;
    }

    public static String getType (Row row) {
        String input = row.getAs( "Held_At" );        //only for the rows where the facility is specifed,
        if (StringUtils.isNotBlank( input )) {                //call the type "jail"
            return "Jail";
        }
        return null;
    }

    //there are several entries of "0.0" in fields that should be integers
    public static Integer getInt (Object obj) {
        String input = getAsString( obj );

        if (input.equals( "0.0" )) {
            return 0;
        }
            else if (StringUtils.isNotBlank( input )) {
            return Integer.parseInt ( input );
        }
            return null;
    }

}
